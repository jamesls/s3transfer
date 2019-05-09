# Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.
"""High level module for recursive S3 transfers.

This module is a high level module that uses a TransferManager to provide
recursive S3 transfers similar to "aws s3 cp --recursive".

Usage
=====

You can use a ``RecursiveDownload()`` object by providing with a manager.
The lifecycle of a manager is separate from the lifecycle of a
``RecursiveDownload()``.  This class doesn't shutdown the manager.

.. code-block:: python

    import boto3
    from s3transfer.manager import TransferManager, TransferConfig
    client = boto3.client('s3', 'us-west-2')
    config = TransferConfig()
    manager = TransferManager(client, config)

    with RecursiveDownload(manager) as d:
        r = d.download_directory(bucket='foo', prefix='bar/',
                                 directory='/tmp/out/')
        r2 = d.download_directory(bucket='baz', prefix='other/',
                                  directory='/tmp/out2/')
        r.result()
        r2.result()

This will download everything from ``s3://foo/bar/`` to ``/tmp/out``
as well as downloading ``s3://baz/other`` to ``/tmp/out2``.  These
transfers will happen in parallel.

"""
import logging
import threading
import errno
import os

from s3transfer.compat import MAXINT
from s3transfer.subscribers import BaseSubscriber

logger = logging.getLogger(__name__)


# IMPLEMENTATION NOTES
#
# This module uses its own orchestration logic instead of being coupled
# to s3transfer.futures classes.
#
# Downloading from s3 needs a two-tierred orchestration.
# 1. List all the objects from S3 and queue them for download
# 2. Wait for all the queued S3 objects to finish downloading.
#
# We return back to the user a future object (defined in this module)
# that encapsulates waiting for both tiers listed above.  That way
# a user can just call ``.result()``.
#
# Each of the two steps listed above are futures.  The future we return back
# to the user just waits for each future to complete before saying that
# itself is done.
#
# That's the basic idea...but there's a few complications.
# First an individual download object task has an asynchronous task.
# That is, when you call manager.download() and you get a future back,
# the actual download has not begun.  Instead we submitted a request
# to a separate executor where it's workers then submit tasks that do
# the actual download from S3 (if you're curious about this, it's because
# when we ask to download an object from S3, we don't know yet what the
# best way to download the object is, we may use multipart downloads
# or a single get object call).
#
# This makes things complicated for us because we need some way to know
# when all the downloads are complete.  Because the task submissions are
# all non-deterministic, the only thing we can reliably do is to track
# how many downloads we queued, and then wait until we see that many
# downloads complete.


class RecursiveDownload(object):

    def __init__(self, manager, object_lister):
        """

        :param manager: s3transfer.manager.TransferManager
        :type object_lister: ObjectLister
        """
        self._manager = manager
        self._object_lister = object_lister

    def download_directory(self, bucket, prefix, directory, extra_args=None,
                           subscribers=None):
        """Recursively download objects from S3 to a local directory.

        :type bucket: str
        :param bucket: The name of the bucket to download from

        :type prefix: str
        :param key: The name of the key prefix to download from

        :type directory: str
        :param fileobj: The name of a file to download or a seekable file-like
            object to download. It is recommended to use a filename because
            file-like objects may result in higher memory usage.

        :type extra_args: dict
        :param extra_args: Extra arguments that may be passed to the
            client operation

        :type subscribers: list(s3transfer.subscribers.BaseSubscriber)
        :param subscribers: The list of subscribers to be invoked in the
            order provided based on the event emit during the process of
            the transfer request.

        :rtype: s3transfer.futures.TransferFuture
        :returns: Transfer future representing the download
        """
        if extra_args is None:
            extra_args = {}
        if subscribers is None:
            subscribers = []
        lister_future = RFuture()
        lister_kwargs = {
            'bucket': bucket,
            'prefix': prefix,
            'directory': directory,
        }
        lister_thread = ObjectListerThread(self._object_lister,
                                           self._manager,
                                           lister_future,
                                           lister_kwargs)
        lister_thread.start()
        return RecursiveDownloadFuture(lister_future)


class RecursiveDownloadFuture(object):
    def __init__(self, lister_future):
        self._lister_future = lister_future
        self._finished_event = threading.Event()

    def done(self):
        return self._finished_event.is_set()

    def result(self):
        if not self._finished_event.is_set():
            logger.debug("RecursiveDownloadFuture.result() called, waiting "
                         "for progress tracker's results.")
            progress_tracker = self._lister_future.result()
            logger.debug("ProgressTracker finished, done queueing download "
                         "tasks, registering zero futures callback.")
            progress_tracker.set_zero_futures_callback(
                self._finished_event.set)
            logger.debug("Waiting for finished event signaling all downloads "
                         "are completed.")
            self._finished_event.wait(MAXINT)
            logger.debug("Finished event has been set, all downloads are "
                         "completed.")
        return True

    def cancel(self):
        raise NotImplementedError("cancel()")


# This class runs in the ObjectListerThread()'s thread, not the main thread.
class ProgressTracker(object):
    def __init__(self):
        self._futures = set()
        self._locked = False
        self._zero_futures_callback = None
        self._lock_trigger_value = None
        self._tracked_futures_count = 0
        # Used to coordinate locking this object and registering
        # a callback
        self._callback_lock = threading.Lock()

    def add_future_to_track(self, future):
        if not self._locked:
            self._futures.add(future)
            self._tracked_futures_count += 1
            logger.debug("Add future to progress tracker, tracked "
                         "future count: %s", self._tracked_futures_count)
            self._check_can_lock()
        else:
            # TODO: Better exception
            logger.error("Can't add future %s with call args: %s, "
                         "tracker has been locked.",
                         future, future.meta.call_args)
            raise RuntimeError("Can't add future, tracker has been locked.")

    def remove_future_to_track(self, future):
        self._futures.remove(future)
        logger.debug("Removed future %s, remaining futures: %s, "
                     "(locked: %s)", future, len(self._futures), self._locked)
        if self._locked and not self._futures:
            if self._zero_futures_callback is not None:
                self._zero_futures_callback()

    def set_lock_trigger_value(self, amount):
        logger.debug("Progress lock trigger value set to %s", amount)
        self._lock_trigger_value = amount
        # We may already be at the lock trigger value so we should check.
        self._check_can_lock()

    def _check_can_lock(self):
        if self._tracked_futures_count == self._lock_trigger_value:
            logger.debug("Lock trigger value reached, locking progress "
                            "tracker.")
            self._lock()

    def _lock(self):
        """Prevent futures from being added.

        Postcondition: Once this object has been locked, attempting to
            add anymore future will raise an exception.  You are
            guaranteed that the number of pending futures will never
            increase.
        """
        with self._callback_lock:
            self._locked = True
        logger.debug("ProgressTracker has been locked, %s futures remaining.",
                     len(self._futures))

    def set_zero_futures_callback(self, callback):
        """Add a callback when all pending futures are done."""
        with self._callback_lock:
            self._zero_futures_callback = callback
            # It's possible when we register a zero futures callback that
            # all the tracked futures are done.
            if self.done():
                self._zero_futures_callback()

    def done(self):
        return not self._futures


# This adheres to the subscribers interface.
# This serves as an adapter to the ProgressTracker class, which is
# decoupled from the callbacks and is an interace this module controls.
class GetObjectsStatusTrack(BaseSubscriber):
    def __init__(self, tracker):
        """

        :type tracker: GetObjectsStatusTrack
        """
        self._tracker = tracker

    def on_queued(self, future, **kwargs):
        logger.debug("Adding future to tracking list with call args: %s",
                     future.meta.call_args)
        self._tracker.add_future_to_track(future)

    def on_done(self, future, **kwargs):
        logger.debug("Future done, removing future with call args: %s",
                     future.meta.call_args)
        self._tracker.remove_future_to_track(future)


class ObjectListerThread(threading.Thread):
    def __init__(self, object_lister, manager, lister_future, lister_kwargs):
        super(ObjectListerThread, self).__init__()
        self._object_lister = object_lister
        self._manager = manager
        self._lister_kwargs = lister_kwargs
        # This is used to communicate with the thing that starts
        # this thread when we're done listing all the objects.
        self._lister_future = lister_future

    def run(self):
        progress_tracker = ProgressTracker()
        bucket = self._lister_kwargs['bucket']
        prefix = self._lister_kwargs['prefix']
        directory = self._lister_kwargs['directory']
        amount = 0
        for key in self._object_lister.list_objects(bucket=bucket,
                                                    prefix=prefix):
            file_path = key['Key'][len(prefix):]
            full_path = os.path.join(directory, file_path)
            print(f"{bucket}/{key['Key']} -> {full_path}")
            # We don't need the reference to the future because the
            # bookkeeping is all done through the GetObjectsStatusTrack
            # subscriber.
            self._manager.download(
                bucket=bucket, key=key['Key'], fileobj=full_path,
                subscribers=[
                    ProvideSizeSubscriber(size=key['Size']),
                    DirectoryCreatorSubscriber(),
                    GetObjectsStatusTrack(progress_tracker),
                ]
            )
            amount += 1
        progress_tracker.set_lock_trigger_value(amount)
        self._lister_future.set_result(progress_tracker)


# This is similar to s3transfer.futures.BaseTransferFuture except
# it doesn't have a `.meta` property because the interface for that object
# doesn't make sense in this context (e.g transfer size).  We can revisit
# this if we think if makes sense.
class RFuture(object):
    def __init__(self):
        self._done = False
        self._result = None
        self._exception = None
        self._done_event = threading.Event()

    def done(self):
        return self._done

    def result(self):
        self._done_event.wait(MAXINT)
        if self._exception is not None:
            raise self._exception
        return self._result

    def cancel(self):
        raise NotImplementedError("cancel()")

    def set_result(self, result):
        # TODO: Only allow set_result/set_exception to be set once.
        self._done = True
        self._result = result
        self._done_event.set()

    def set_exception(self, exception):
        self._done = True
        self._exception = exception
        self._done_event.set()


# The RFuture is something we return back to the user.  We use this to
# set the state of the future when it flips to done.
class _FutureStateController(object):
    pass


class ObjectLister(object):
    """Recursively list objects in S3 from a starting prefix."""
    def __init__(self, client):
        self._client = client

    def list_objects(self, bucket, prefix=None, filters=None):
        """List objects for a given bucket and prefix.

        :param bucket: The S3 bucket
        :param prefix:  The S3 key prefix.  If this value is None, then no
            prefix will be used and all objects will be listed (subject to
            the filters parameter).
        :param listers: A list of callables.  Each callable should accept
            a key object.  If any filter returns False, then the object is
            not returned.

        :returns: An iterable of S3 objects.  Each S3 object is a dictionary
            that contains these keys: ``Key``, ``LastModified``, ``ETag``,
            ``Size``.  For example::

                {
                    "Key": "foobar",
                    "LastModified": "2019-07-30T20:57:10+00:00",
                    "ETag": "\"856679258e20442073bbd7b967df199a\"",
                    "Size": 43,
                }

        """
        paginator = self._client.get_paginator('list_objects')
        kwargs = {'Bucket': bucket}
        if prefix is not None:
            kwargs['Prefix'] = prefix
        for page in paginator.paginate(**kwargs):
            for key in page['Contents']:
                # Maybe this isn't useful, but I'm trying to reduce what
                # we return to make it possbile to implement other
                # implementations of this class.
                key.pop('Owner')
                key.pop('StorageClass')
                should_yield_key = True
                if filters is not None:
                    for f in filters:
                        if not f(key):
                            should_yield_key = False
                            break
                if should_yield_key:
                    yield key


class ProvideSizeSubscriber(BaseSubscriber):
    """
    A subscriber which provides the transfer size before it's queued.
    """
    def __init__(self, size):
        self.size = size

    def on_queued(self, future, **kwargs):
        future.meta.provide_transfer_size(self.size)


class DirectoryCreatorSubscriber(BaseSubscriber):
    """Creates a directory to download if it does not exist"""
    def on_queued(self, future, **kwargs):
        d = os.path.dirname(future.meta.call_args.fileobj)
        try:
            # TODO: Add LRU caching to avoid unnecessary stats() calls.
            if not os.path.exists(d):
                os.makedirs(d)
        except OSError as e:
            if not e.errno == errno.EEXIST:
                # TODO: Specific exception class.
                raise RuntimeError(
                    "Could not create directory %s: %s" % (d, e))

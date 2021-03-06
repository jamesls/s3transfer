# Copyright 2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import time

from concurrent.futures import CancelledError

from botocore.compat import six
from tests import RecordingSubscriber, NonSeekableReader
from tests.integration import BaseTransferManagerIntegTest
from s3transfer.manager import TransferConfig


class TestUpload(BaseTransferManagerIntegTest):
    def setUp(self):
        super(TestUpload, self).setUp()
        self.multipart_threshold = 5 * 1024 * 1024
        self.config = TransferConfig(
            multipart_threshold=self.multipart_threshold)

    def get_input_fileobj(self, size, name=''):
        return self.files.create_file_with_size(name, size)

    def test_upload_below_threshold(self):
        transfer_manager = self.create_transfer_manager(self.config)
        file = self.get_input_fileobj(size=1024 * 1024, name='1mb.txt')
        future = transfer_manager.upload(file, self.bucket_name, '1mb.txt')
        self.addCleanup(self.delete_object, '1mb.txt')

        future.result()
        self.assertTrue(self.object_exists('1mb.txt'))

    def test_upload_above_threshold(self):
        transfer_manager = self.create_transfer_manager(self.config)
        file = self.get_input_fileobj(size=20 * 1024 * 1024, name='20mb.txt')
        future = transfer_manager.upload(
            file, self.bucket_name, '20mb.txt')
        self.addCleanup(self.delete_object, '20mb.txt')

        future.result()
        self.assertTrue(self.object_exists('20mb.txt'))

    def test_large_upload_exits_quicky_on_exception(self):
        transfer_manager = self.create_transfer_manager(self.config)

        filename = self.get_input_fileobj(
            name='foo.txt', size=20 * 1024 * 1024)

        sleep_time = 0.25
        try:
            with transfer_manager:
                start_time = time.time()
                future = transfer_manager.upload(
                    filename, self.bucket_name, '20mb.txt')
                # Sleep for a little to get the transfer process going
                time.sleep(sleep_time)
                # Raise an exception which should cause the preceeding
                # download to cancel and exit quickly
                raise KeyboardInterrupt()
        except KeyboardInterrupt:
            pass
        end_time = time.time()
        # The maximum time allowed for the transfer manager to exit.
        # This means that it should take less than a couple second after
        # sleeping to exit.
        max_allowed_exit_time = sleep_time + 2
        self.assertTrue(end_time - start_time < max_allowed_exit_time)

        try:
            future.result()
            self.skipTest(
                'Upload completed before interrupted and therefore '
                'could not cancel the upload')
        except CancelledError:
            # If the transfer did get cancelled,
            # make sure the object does not exist.
            self.assertFalse(self.object_exists('20mb.txt'))

    def test_progress_subscribers_on_upload(self):
        subscriber = RecordingSubscriber()
        transfer_manager = self.create_transfer_manager(self.config)
        file = self.get_input_fileobj(size=20 * 1024 * 1024, name='20mb.txt')
        future = transfer_manager.upload(
            file, self.bucket_name, '20mb.txt',
            subscribers=[subscriber])
        self.addCleanup(self.delete_object, '20mb.txt')

        future.result()
        # The callback should have been called enough times such that
        # the total amount of bytes we've seen (via the "amount"
        # arg to the callback function) should be the size
        # of the file we uploaded.
        self.assertEqual(subscriber.calculate_bytes_seen(), 20 * 1024 * 1024)


class TestUploadSeekableStream(TestUpload):
    def get_input_fileobj(self, size, name=''):
        return six.BytesIO(b'0' * size)


class TestUploadNonSeekableStream(TestUpload):
    def get_input_fileobj(self, size, name=''):
        return NonSeekableReader(b'0' * size)

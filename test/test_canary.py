# Copyright 2017 Douglas G. Moore, Harrison Smith. All rights reserved.
# Use of this source code is governed by a MIT
# license that can be found in the LICENSE file.
import unittest

class TestCanary(unittest.TestCase):
    """
    A canary test case
    """
    def test_canary(self):
        """
        A canary test
        """
        self.assertEqual(4, 1+3)

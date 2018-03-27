# -*- coding: utf-8 -*-
#
# Copyright 2012-2015 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
import shutil
import time

import luigi1


class MyExternal(luigi1.ExternalTask):

    def complete(self):
        return False


class Foo(luigi1.Task):

    def run(self):
        print "Running Foo"

    def requires(self):
        #        yield MyExternal()
        for i in xrange(10):
            yield Bar(i)


class Bar(luigi1.Task):
    num = luigi1.IntParameter()

    def run(self):
        time.sleep(1)
        self.output().open('w').close()

    def output(self):
        """
        Returns the target output for this task.

        :return: the target output for this task.
        :rtype: object (:py:class:`~luigi.target.Target`)
        """
        time.sleep(1)
        return luigi1.LocalTarget('/tmp/bar/%d' % self.num)


if __name__ == "__main__":
    if os.path.exists('/tmp/bar'):
        shutil.rmtree('/tmp/bar')

    luigi1.run(['--task', 'Foo', '--workers', '2'], use_optparse=True)

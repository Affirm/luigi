# Super exciting license
from cStringIO import StringIO

from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol, RawProtocol
import mock
from mock import patch

from luigi.s3 import S3Target, S3Client
# from luigi.contrib import mrjob
import luigi.contrib
from luigi.contrib.mrjob import MRJobTask
from luigi.task import flatten
import luigi
import luigi.interface
import luigi.worker
import luigi.scheduler
import unittest
import tempfile
import os

# moto does not yet work with
# python 2.6. Until it does,
# disable these tests in python2.6
try:
    from moto import mock_s3
except ImportError:
    # https://github.com/spulec/moto/issues/29
    print('Skipping %s because moto does not install properly before '
          'python2.7' % __file__)
    from luigi.mock import skip
    mock_s3 = skip

AWS_ACCESS_KEY = "XXXXXXXXXXXXXXXXXXXX"
AWS_SECRET_KEY = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"


class MRWordFrequencyCount(MRJob):

    def mapper(self, _, line):
        yield "chars", len(line)
        yield "words", len(line.split())
        yield "lines", 1

    def reducer(self, key, values):
        yield key, sum(values)


class MRAddIncrementNumbers(MRJob):

    # INPUT_PROTOCOL = RawValueProtocol
    OUTPUT_PROTOCOL = RawValueProtocol

    def mapper(self, _, number):
        yield None, int(number) + 5

    def reducer(self, _, values):

        yield None, sum(map(int, values))


class DebugTarget(luigi.Target):

    def __init__(self, data):
        self.data = data

    def __call__(self, *args, **kwargs):
        return self.data


class DataDump(luigi.ExternalTask):

    def run(self):
        pass

    def complete(self):
        return True

    def output(self):
        return None


class WordCountTask(MRJobTask):
    job_class = MRWordFrequencyCount

    def requires(self):
        return DataDump()


class AddIncrementTask1a(MRJobTask):
    job_class = MRAddIncrementNumbers
    # job_args = luigi.Parameter()

    def requires(self):
        return DataDump()


class AddIncrementTask1b(MRJobTask):
    job_class = MRAddIncrementNumbers
    # job_args = luigi.Parameter()

    def requires(self):
        return DataDump()


class AddIncrementTask2(MRJobTask):
    job_class = MRAddIncrementNumbers
    misc_options = {'parse_output': True}

    def requires(self):
        return AddIncrementTask1a(), AddIncrementTask1b()


class AddIncrementTask3(MRJobTask):
    job_class = MRAddIncrementNumbers
    misc_options = {'parse_output': True}
    s3_bucket = ''

    def requires(self):
        return AddIncrementTask1a(job_args=['-r', 'emr', '--no-conf', '--strict-protocols',
                                            '--output-dir=s3://mybucket/wc_out/']),\
               AddIncrementTask1b(job_args=['-r', 'emr', '--no-conf', '--strict-protocols',
                                            '--output-dir=s3://mybucket/wc_out/'])




class MRJobTaskTest(unittest.TestCase):

    def setUp(self):
        self.default_args = ['-r', 'inline', '--no-conf', '--strict-protocols', '-']
        self.m_data_output = patch.object(DataDump, 'output', autospec=True).start()
        self.m_run_emr = patch.object(MRJobTask, 'run_emr', autospec=True).start()
        self.m_run_emr.side_effect = lambda self_, _: self.fake_emr(self_)

        self.client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)

        f = tempfile.NamedTemporaryFile(mode='wb', delete=False)
        self.tempFileContents = (
            '\n'.join(["hello", "hello", "hello"])
        )
        self.tempFilePath = f.name
        f.write(self.tempFileContents)
        f.close()

    def tearDown(self):
        os.remove(self.tempFilePath)

    def run_task_with_input(self, task, input_data):
        self.m_data_output.return_value = input_data
        task.run()

    def test_run_task(self):
        task = WordCountTask()
        task.misc_options = {'parse_output': True}
        self.run_task_with_input(task, ["hello", "hello", "hello"])

        result = task.output()
        comparison = [('chars', 15), ('lines', 3), ('words', 3)]
        self.assertEqual(result, comparison)

    def fake_emr(self, task):
        args = task.job_args
        args[args.index('emr')] = 'inline'
        job = task.job_class(args)
        stdin = StringIO()
        if isinstance(task.input(), list):
            inlines = []
            for t in task.input():
                inlines.append(t.open().read().split())
            inlines = flatten(inlines)
        else:
            inlines = task.input().open().read().split()

        inlines = '\n'.join(inlines)
        stdin.write(inlines)
        stdin.seek(0)
        job.sandbox(stdin=stdin)
        results = ''
        task.job_options['runner'] = 'inline'
        with job.make_runner() as runner:
            runner.run()
            for line in runner.stream_output():
                results += line
        self.client.put_string(results, task.s3_output_path)
        task.job_options['runner'] = 'emr'
        return results

    @mock_s3
    def test_fake_emr(self):
        bucket = 'mybucket'
        output_path = 's3://mybucket/wc_out/'
        input_path = 's3://mybucket/wc_in/'
        self.client.s3.create_bucket(bucket)
        self.client.put(self.tempFilePath, input_path)

        args = ['-r', 'emr', '--no-conf', '--strict-protocols', '--output-dir=' + output_path]
        task = WordCountTask()
        task.job_args = args
        self.run_task_with_input(task, S3Target(input_path, client=self.client))

        read_file = task.output().open()
        file_str = read_file.read()
        self.assertEqual(file_str, '"chars"\t15\n"lines"\t3\n"words"\t3\n')

    def test_multiple_tasks(self):
        self.m_data_output.return_value = ['1', '2', '3', '4']
        task = AddIncrementTask2()
        luigi.interface.setup_interface_logging()
        sch = luigi.scheduler.CentralPlannerScheduler()
        w = luigi.worker.Worker(scheduler=sch)
        w.add(task)
        w.run()
        self.assertEqual(task.output(), [(None, '70\n')])

    @mock_s3
    def test_multiple_fake_emr(self):
        bucket = 'mybucket'
        output_path = 's3://mybucket/wc_out/'
        input_path = 's3://mybucket/wc_in/'
        self.client.s3.create_bucket(bucket)
        self.client.put_string('\n'.join(['1', '2', '3', '4']), input_path)

        self.m_data_output.return_value = S3Target(input_path, client=self.client)
        task = AddIncrementTask3(job_args=['-r', 'emr', '--no-conf',
                                           '--strict-protocols', '--output-dir=' + output_path])

        luigi.interface.setup_interface_logging()
        sch = luigi.scheduler.CentralPlannerScheduler()
        w = luigi.worker.Worker(scheduler=sch)
        w.add(task)
        w.run()
        read_file = task.output().open()
        file_str = read_file.read()
        self.assertEqual(file_str, '70\n')

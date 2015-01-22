# Some awesome license

# import mrjob
from __future__ import absolute_import

from cStringIO import StringIO
from luigi.task import flatten
import logging
import luigi
import luigi.s3
from datetime import datetime


logger = logging.getLogger('luigi-interface')


class MRJobTask(luigi.Task):
    job_class = None
    job_args = luigi.Parameter(default=None)
    job_options = {}
    job_results = None
    s3_output_path = None
    misc_options = {}

    def run_inline(self, job):
        print 'gothereherhhe', self
        stdin = StringIO()
        inlines = self.input()
        inlines = flatten(inlines)
        inlines = ('\n'.join(inlines)).replace('\n\n', '\n')
        stdin.write(inlines)
        stdin.seek(0)
        job.sandbox(stdin=stdin)
        results = []
        with job.make_runner() as runner:
            runner.run()
            for line in runner.stream_output():
                if self.misc_options.get('parse_output', False):
                    key, value = job.parse_output_line(line)
                    results.append((key, value))
                else:
                    results.append(line)
        return results

    def run_emr(self, job):
        results = []
        with job.make_runner() as runner:
            runner.run()
            for line in runner.stream_output():
                key, value = job.parse_output_line(line)
                results.append((key, value))

    def run(self):
        self.job_args = list(self.job_args or []) or ['-r', 'inline', '--no-conf', '--strict-protocols', '-']

        logger.info('Running mrjob task with arguments: %s' % ', '.join(self.job_args))

        job = self.job_class(self.job_args)
        self.job_options = vars(vars(job)['options'])

        runner = self.job_options['runner']

        if runner == 'inline':
            self.job_results = self.run_inline(job)

        elif runner == 'emr':

            for arg in self.job_args:
                if '--output-dir' in arg:
                    self.s3_output_path = arg.split('=', 1)[-1]

            if not self.s3_output_path:
                self.s3_output_path = str(self.task_id) + '_' + datetime.utcnow().strftime('%Y-%m-%d-%H-%M-%S')
                logger.info('You did not specify an output s3 path. It will be automatically assigned as: %s' %
                            self.s3_output_path)

            self.run_emr(job)
        else:
            raise NotImplementedError
        logger.info('Finished running mrjob')

    def complete(self):
        return self.job_results is not None or self.s3_output_path is not None

    def output(self):
        runner = self.job_options['runner']
        if runner == 'inline':
            return self.job_results
        elif runner == 'emr':
            return luigi.s3.S3Target(self.s3_output_path)


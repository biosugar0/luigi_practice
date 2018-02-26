#!/usr/bin/env python3

import luigi
from time import sleep


class First(luigi.Task):
    """
    test -> local
    """
    param1 = luigi.Parameter()
    param2 = luigi.Parameter()
    sec = luigi.IntParameter(default=60)

    def requires(self):
        return []

    def output(self):
        return luigi.LocalTarget(path="./test1.txt")

    def run(self):
        fp = self.output().open("w")
        sleep(self.sec)
        fp.write(self.param1 + "\n" + self.param2 + "\n")
        fp.close()


class Second(luigi.Task):
    param1 = luigi.Parameter()
    param2 = luigi.Parameter()
    sec = luigi.IntParameter(default=60)

    def requires(self):
        return [First()]

    def output(self):
        return luigi.LocalTarget("test2.txt")
#

    def run(self):
        fin = self.input()[0].open()
        fout = self.output().open('w')
        for line in fin:
            line = line.strip()
            sleep(self.sec)
            out = "\t".join([line, self.param2,
                             self.param1])
            fout.write(out + "\n")
        fin.close()
        fout.close()


class Alpha(luigi.Task):
    sec = luigi.IntParameter(default=60)

    def requires(self):
        return [Second()]

    def output(self):
        return luigi.LocalTarget("test3.txt")
#

    def run(self):
        fin = self.input()[0].open()
        fout = self.output().open('w')
        for line in fin:
            line = line.strip()
            sleep(self.sec)
            out = "\t".join([line, "Alpha"])
            fout.write(out + "\n")
        fin.close()
        fout.close()


class Beta(luigi.Task):
    sec = luigi.IntParameter(default=60)

    def requires(self):
        return [Alpha()]

    def output(self):
        return luigi.LocalTarget("test4.txt")
#

    def run(self):
        fin = self.input()[0].open()
        fout = self.output().open('w')
        for line in fin:
            line = line.strip()
            sleep(self.sec)
            out = "\t".join([line, "Beta"])
            fout.write(out + "\n")
        fin.close()
        fout.close()


if __name__ == '__main__':
    luigi.run()

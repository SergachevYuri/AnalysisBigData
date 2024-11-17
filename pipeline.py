import luigi
import os
import wget


class DownloadDataset(luigi.Task):
    dataset_name = luigi.Parameter(default='GSE68849')
    output_dir = luigi.Parameter(default='data')

    def output(self):
        return luigi.LocalTarget(os.path.join(self.output_dir, f'{self.dataset_name}_RAW.tar'))


    def run(self):
        os.makedirs(self.output_dir, exist_ok=True)
        url = f'https://www.ncbi.nlm.nih.gov/geo/download/?acc={self.dataset_name}&format=file'
        wget.download(url, out=self.output_dir)







if __name__ == '__main__':
    luigi.run(['DownloadDataset', '--local-scheduler'])
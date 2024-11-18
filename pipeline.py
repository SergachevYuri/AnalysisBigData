import luigi
import os
import wget
import tarfile
import gzip
import glob
import io
import pandas as pd

class DownloadDatasetTask(luigi.Task):
    dataset_name = luigi.Parameter(default='GSE68849')
    
    def output(self):
        return luigi.LocalTarget(os.path.join('data', 'RAW', self.dataset_name, f'{self.dataset_name}_RAW.tar'))

    def run(self):
        output_dir = os.path.join('data', 'RAW', self.dataset_name)
        os.makedirs(output_dir, exist_ok=True)
        
        url = f'https://www.ncbi.nlm.nih.gov/geo/download/?acc={self.dataset_name}&format=file'
        wget.download(url, out=str(self.output().path))


class ExtractTarTask(luigi.Task):
    dataset_name = luigi.Parameter(default='GSE68849')
    output_dir = luigi.Parameter(default='data')

    def requires(self):
        return DownloadDatasetTask(dataset_name=self.dataset_name)

    def output(self):
        return luigi.LocalTarget(os.path.join(self.output_dir, 'input',self.dataset_name))

    def run(self):
        with tarfile.open(self.input().path, 'r') as tar:
            tar.extractall(str(self.output().path))


class ExtractGzFilesTask(luigi.Task):
    dataset_name = luigi.Parameter(default='GSE68849')
    output_dir = luigi.Parameter(default='data')

    def requires(self):
        return ExtractTarTask(dataset_name=self.dataset_name, output_dir=self.output_dir)

    def output(self):
        return luigi.LocalTarget(os.path.join(self.output_dir, 'input' ,self.dataset_name, '.gz_extraction_complete'))

    def run(self):
        input_dir = self.input().path
        gz_files = glob.glob(os.path.join(input_dir, '**', '*.gz'), recursive=True)
        
        for gz_file in gz_files:
            output_file = gz_file[:-3]
            
            with gzip.open(gz_file, 'rb') as f_in:
                with open(output_file, 'wb') as f_out:
                    f_out.write(f_in.read())
            
            os.remove(gz_file)
        
        with open(self.output().path, 'w') as f:
            f.write('GZ extraction completed')


class ParseTableTask(luigi.Task):
    dataset_name = luigi.Parameter(default='GSE68849')
    output_dir = luigi.Parameter(default='data')

    def requires(self):
        return ExtractGzFilesTask(dataset_name=self.dataset_name, output_dir=self.output_dir)
    
    def output(self):
        return luigi.LocalTarget(os.path.join(self.output_dir, 'output' ,self.dataset_name, '.table_parsing_complete'))

    def run(self):
        input_dir = os.path.join('data', 'output', self.dataset_name)
        os.makedirs(input_dir, exist_ok=True)
        
        table_files = glob.glob(os.path.join(self.output_dir, 'input', self.dataset_name, '**', '*.txt'), recursive=True)

        for table_file in table_files:
            with open(self.output().path, 'a') as f:
                f.write(table_file)
            




if __name__ == '__main__':
    """
    Run the pipeline with a specific dataset name:
    python pipeline.py ExtractGzFilesTask --dataset-name GSE68849 --local-scheduler
    """
    luigi.run()
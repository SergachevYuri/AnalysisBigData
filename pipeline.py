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
        return luigi.LocalTarget(os.path.join(self.output_dir, f'{self.dataset_name}.gz_extraction_complete'))

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
        return luigi.LocalTarget(os.path.join(self.output_dir, f'{self.dataset_name}.table_parsing_complete'))

    def run(self):
        input_dir = os.path.join(self.output_dir, 'input', self.dataset_name)
        output_dir = os.path.join(self.output_dir, 'output', self.dataset_name)
        os.makedirs(output_dir, exist_ok=True)
        
        files_in_dir = os.listdir(input_dir)
        for file_name in files_in_dir:
            if not file_name.endswith('.txt'):
                continue
            file_path = os.path.join(input_dir, file_name)
            if os.path.isfile(file_path):
                print(f'Processing file: {file_path}')
                dfs = {}
                with open(file_path, 'r') as f:
                    write_key = None
                    fio = io.StringIO()
                    for l in f.readlines():
                        if l.startswith('['):
                            if write_key:
                                fio.seek(0)
                                header = None if write_key == 'Heading' else 'infer'
                                dfs[write_key] = pd.read_csv(fio, sep='\t', header=header)
                                print(f'Parsed section: {write_key}')
                            fio = io.StringIO()
                            write_key = l.strip('[]\n')
                            continue
                        if write_key:
                            fio.write(l)
                    if write_key:
                        fio.seek(0)
                        dfs[write_key] = pd.read_csv(fio, sep='\t')
                        print(f'Parsed section: {write_key}')
                file_base_name = os.path.splitext(file_name)[0]
                output_path = os.path.join(output_dir, file_base_name)
                os.makedirs(output_path, exist_ok=True)
                for key, df in dfs.items():
                    output_file = os.path.join(output_path, f'{key}.tsv')
                    os.makedirs(os.path.dirname(output_file), exist_ok=True)
                    df.to_csv(output_file, sep='\t', index=False)
                    print(f'Saved {output_file}')
        with open(self.output().path, 'w') as f:
            f.write('Parsing tables complete')
        print(f'Parsing complete. Output written to {self.output().path}')


class ProbesTableTask(luigi.Task):
    dataset_name = luigi.Parameter(default='GSE68849')
    output_dir = luigi.Parameter(default='data')
    
    def requires(self):
        return ParseTableTask(dataset_name=self.dataset_name, output_dir=self.output_dir)

    def output(self):
        return luigi.LocalTarget(os.path.join(self.output_dir, f'{self.dataset_name}_probes_clean'))

    def run(self):
        columns_to_drop = ['Definition', 'Ontology_Component', 'Ontology_Process', 'Ontology_Function', 'Synonyms', 'Obsolete_Probe_Id', 'Probe_Sequence'
]
        output_dir = os.path.join(self.output_dir, 'output', self.dataset_name)

        print(f'Processing files in {output_dir}')
        dirs = [d for d in os.listdir(output_dir) if os.path.isdir(os.path.join(output_dir, d))]

        for dir_name in dirs:
            print(f'Processing {dir_name}')
            dir_path = os.path.join(output_dir, dir_name)
            probes_file = os.path.join(dir_path, 'Probes.tsv')
            if os.path.exists(probes_file):
                df = pd.read_csv(probes_file, sep='\t')
                df_reduced = df.drop(columns=columns_to_drop, errors='ignore')
                output_file = os.path.join(dir_path, 'Probes_clean.tsv')
                df_reduced.to_csv(output_file, sep='\t', index=False)
        with open(self.output().path, 'w') as f:
            f.write('Probes table cleaned')

class Cleanup(luigi.Task):
    dataset_name = luigi.Parameter(default='GSE68849')
    output_dir = luigi.Parameter(default='data')

    def requires(self):
        return ProbesTableTask(dataset_name=self.dataset_name, output_dir=self.output_dir)

    def output(self):
        return luigi.LocalTarget(os.path.join(self.output_dir, f'{self.dataset_name}_cleanup_done'))

    def run(self):
        output_dir = os.path.join(self.output_dir, 'input', self.dataset_name)
        files_in_dir = os.listdir(output_dir)
        for file_name in files_in_dir:
            file_path = os.path.join(output_dir, file_name)
            if os.path.isfile(file_path) and file_name.endswith('.txt'):
                os.remove(file_path)
        with open(self.output().path, 'w') as f:
            f.write('Cleanup complete')

class Pipeline(luigi.Task):
    dataset_name = luigi.Parameter(default='GSE68849')
    output_dir = luigi.Parameter(default='data')

    def requires(self):
        return Cleanup(dataset_name=self.dataset_name, output_dir=self.output_dir)

    def output(self):
        return luigi.LocalTarget(os.path.join(self.output_dir, f'{self.dataset_name}_pipeline_complete'))

    def run(self):
        with open(self.output().path, 'w') as f:
            f.write('Pipeline completed successfully')

if __name__ == '__main__':
    luigi.run(['Pipeline', '--local-scheduler'])
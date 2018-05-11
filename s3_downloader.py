import boto3
from boto3 import Session
import csv
import os
import pandas as pd
import pathlib

from setting import *

class S3_Downloader:
    def __init__(self):
        self.setting()
        self.download_file_path_list = []
        self.file_size_list = []

        self.msg = "===== {} ====="

        self.number_take_from_on = 5

        self.output_dir_name = "output"
        self.make_output_dir()

        self.output_list_name = "output_list"

    def setting(self):
        self.session = Session(aws_access_key_id=ACCESS_KEY,
                                aws_secret_access_key=SECRET_KEY)
        self.s3 = self.session.resource("s3")

        # self.s3 = boto3.resource("s3")
        self.bucket_name = BUCKET_NAME
        print("region_name: {}".format(boto3.DEFAULT_SESSION))
        self.bucket = self.s3.Bucket(self.bucket_name)

        # self.s3client = Session().client("s3")
        self.s3client = self.session.client("s3")

    def print_object_info(self, obj):
        print(obj)
        print(obj.key)
        print(obj.size)

    def check_all_items(self, verbose=True):
        msg = self.msg.format("[check_all_items]")
        print(msg)
        for cnt, obj in enumerate(self.bucket.objects.all()):
            if cnt <= 10:
                print("===")
                self.print_object_info(obj)

            self.download_file_path_list.append(obj.key)
            self.file_size_list.append(obj.size)

        print("*"*len(msg))

    def check_prefix_items(self):
        msg = self.msg.format("[check_prefix_items]")
        print(msg)
        for cnt, dir_name in enumerate(DIR_LIST):
            response = self.s3client.list_objects(Bucket=self.bucket_name, Prefix=dir_name)

            if "Contents" in response:
                keys = [content["Key"] for content in response["Contents"]]

                print("IsTruncated: {}".format(response["IsTruncated"]))
                if response["IsTruncated"]: # 1000件以上の場合
                    # keys = [obj.key for obj in self.bucket.objects.all() if obj.key.startswith(dir_name)]
                    pass

                for File in FILE_LIST:
                    if File in keys:
                        print(File)
            else:
                print("{} is empty".format(dir_name))

        self.download_file_path_list = FILE_LIST

    def check_items_num(self):
        objects = list(self.bucket.objects.all())
        print("all objects : {}".format(len(objects)))

    def sort_size_each_directory(self):
        self.files_summary_df = pd.DataFrame({"s3_file_path": self.download_file_path_list, "size": self.file_size_list})

        for cnt, dir_name in enumerate(DIR_LIST):
            file_summary_df = self.files_summary_df[self.files_summary_df["s3_file_path"].str.contains(dir_name)].copy()
            file_summary_df = file_summary_df[file_summary_df["size"] > 0]
            file_summary_df = file_summary_df.sort_values(by="size", ascending=False)
            file_summary_df = file_summary_df.reset_index(drop=True)
            file_summary_df = file_summary_df.iloc[:self.number_take_from_on]

            if cnt==0:
                self.file_summary_df_concat = file_summary_df.copy()
            else:
                self.file_summary_df_concat = pd.concat([self.file_summary_df_concat, file_summary_df], axis=0)

        self.file_summary_df_concat = self.file_summary_df_concat.reset_index(drop=True)
        self.download_file_path_list = self.file_summary_df_concat["s3_file_path"].tolist()
        # print(self.download_file_path_list)

    def download(self):
        msg = self.msg.format("[download]")
        print(msg)

        for s3_file_path in self.download_file_path_list:
            print(s3_file_path)
            file_name = self.make_output_dir(file_name = s3_file_path)
            self.bucket.download_file(Key=s3_file_path, Filename=file_name)
        print("*"*len(msg))

    def make_output_dir(self, file_name=""):
        if file_name == "":
            if not os.path.exists(self.output_dir_name):
                os.mkdir(self.output_dir_name)
        else:
            dirname = os.path.dirname(file_name)
            output_dir = os.path.join(self.output_dir_name, dirname)
            pathlib.Path(output_dir).mkdir(parents=True, exist_ok=True)

            return os.path.join(output_dir, os.path.basename(file_name))

    def make_download_list(self):
        self.file_summary_df_concat.to_csv(os.path.join(self.output_dir_name, self.output_list_name) + ".csv")


def main():
    s3d = S3_Downloader()
    s3d.check_all_items(verbose=False)
    s3d.check_prefix_items()
    s3d.sort_size_each_directory()
    s3d.download()
    s3d.make_download_list()


if __name__ == "__main__":
    main()

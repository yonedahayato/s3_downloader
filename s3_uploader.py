from s3_downloader import S3_Downloader

class S3_Uploader(S3_Downloader):
    def __init__(self):
        S3_Downloader.__init__(self)

    def upload(self):
        self.bucket.upload_file("./upload_test.txt", "upload_test_yoneda.txt")

def main():
    su = S3_Uploader()
    su.upload()

if __name__ == "__main__":
    main()

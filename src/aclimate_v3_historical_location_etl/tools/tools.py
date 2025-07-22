from tqdm import tqdm

from .logging_manager import error


class DownloadProgressBar(tqdm):
    def update_to(self, b=1, bsize=1, tsize=None):
        """
        Updates the progress bar for file downloads.

        Args:
            b (int): Number of blocks transferred
            bsize (int): Size of each block
            tsize (int): Total size of the file
        """
        try:
            if tsize is not None:
                self.total = tsize
            self.update(b * bsize - self.n)
        except Exception as e:
            error(
                "Failed to update download progress", component="download", error=str(e)
            )
            raise

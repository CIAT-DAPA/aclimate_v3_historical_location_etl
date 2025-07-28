from typing import Optional

from tqdm import tqdm

from .logging_manager import error


class DownloadProgressBar(tqdm):
    def update_to(
        self, b: int = 1, bsize: int = 1, tsize: Optional[int] = None
    ) -> None:
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

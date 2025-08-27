from pygnon.client import GBFSCollector

if __name__ == "__main__":

    gbfs = GBFSCollector()
    gbfs.gbfs_collection(length_minutes = 60)

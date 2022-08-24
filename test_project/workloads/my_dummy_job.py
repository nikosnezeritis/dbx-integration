
from test_project.common import Workload
import pandas as pd


class MyDummyJob(Workload):
    def _dummy_function(self):
        self.logger.info("This is my first job")
        db = self.conf["output"].get("database", "default")
        self.logger.info(f"db {db}")
        table = self.conf["output"]["table"]
        _data: pd.DataFrame = pd.DataFrame(data=[[1, "yes", False], [2, "no", True]],
                                           columns=["col_1", "col_2", "col_3"])

        df = self.spark.createDataFrame(_data)
        df.write.format("delta").mode("overwrite").saveAsTable(f"{db}.{table}")
        self.logger.info("Dataset successfully written")

    def launch(self):
        self.logger.info("Launching my dummy job")
        self._dummy_function()
        self.logger.info("My dummy job finished!")


def entrypoint():  # pragma: no cover
    job = MyDummyJob()
    job.launch()


if __name__ == "__main__":
    entrypoint()
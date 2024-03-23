import pandas as pd

from commodity_data.downloaders.base_downloader import BaseDownloader, TypeColumn
from commodity_data.series_config import df_index_columns, OmipConfig
from commodity_data.downloaders.omip.omip_data import Omip_Data


class OmipDownloader(BaseDownloader):

    def __init__(self):
        """
        Creates a barchart downloader.
        For the list of symbols to read, uses a default configuration defined in
        commodity_data.downloaders.default_config.py, that can be extended using configuration file with two keys:
        - omip_downloader: where a json/yaml configuration following OmipConfig marshmallow spec defined in
        series_config can be used
        - omip_downloader_replace: True/False value to define whether the configuration must extend the default
        (False, default value) or replace (if True) the available configuration
        """
        super().__init__(name="Omip", config_name="omip_downloader", class_schema=OmipConfig,
                         default_config_field="omip_downloader_use_default")
        # Calculate the absolute minimum date for download
        self.__min_date = min(pd.Timestamp(cfg.download_cfg.start_t) for cfg in self.config)
        self.omip = Omip_Data()

    def min_date(self):
        return self.__min_date

    def _download_date(self, as_of: pd.Timestamp) -> pd.DataFrame:
        dfs = list()
        # for cdty, cdty_config in OmipConfig.commodity_config.items():
        for cfg in self.config:
            cdty = cfg.commodity_cfg.commodity
            self.logger.info(f"Downloading {cdty} for date {self.as_of_str(as_of)}")
            df = self.omip.download_omip_data(self.as_of_str(as_of), **cfg.download_cfg.__dict__)
            if df is None or df.empty:
                continue        # Skip if empty or None
            for c in df_index_columns:
                if c in cfg.commodity_cfg.__dict__:
                    df[c] = getattr(cfg.commodity_cfg, c)
            df['market'] = self.name()
            df['type'] = TypeColumn.close.value

            df = df.drop(columns=['maturity'])
            df = pd.pivot_table(df, values="close", index="as_of", columns=df_index_columns)
            dfs.append(df)
        if dfs:
            return pd.concat(dfs, axis=1)
        else:
            return None






if __name__ == '__main__':
    import matplotlib.pyplot as plt
    omip = OmipDownloader()
    omip.load()
    omip.settle_xs(commodity="Power", area="ES", product="Y", offset=1).plot()
    plt.show()
    # omip.load()
    print(omip.download(pd.Timestamp(2019, 7, 1)))
    omip.roll_expiration()
    omip.load()
    # print(omip.download(pd.Timestamp(2016, 1, 1)))
    #print(omip.download())
    omip.settle_xs(commodity="Power", area="ES", product="Y", offset=1).plot()
    plt.show()

#    update_all()
#    logger.info("Done")

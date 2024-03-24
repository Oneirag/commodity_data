"""
Sample for downloading data and do some plots
"""
from commodity_data.downloaders import OmipDownloader, BarchartDownloader, EEXDownloader, BaseDownloader
import matplotlib.pyplot as plt
# import mpld3


def recreate(downloader: BaseDownloader):
    """Deletes all data in database, asking for confirmation. If data is deleted, downloads data again"""
    if downloader.delete_all_data():
        downloader.download("2024-01-01")


if __name__ == '__main__':
    mpld3_port = 9999
    omip = OmipDownloader()
    eex = EEXDownloader()
    barchart = BarchartDownloader()
    recreate(omip)
    recreate(eex)
    recreate(barchart)
    print(omip.settlement_df)  # Actual data. No need to invoke download()
    print(omip.settle_xs(commodity="Power", product="Y", area="ES", offset=1))
    exit(0)

    df = omip._download_date("2024-03-10")
    omip.download("2020-01-01")  # , force_download=True)
    # omip.download("2024-01-01")
    # omip = OmipDownloader()
    print(omip.settlement_df)  # Actual data. No need to invoke download()

    for product in "YMQD":
        omip.settle_xs(market="Omip", commodity="Power", area="ES", product=product, offset=slice(1, 2)).plot()
        plt.title(product)
        plt.show()
    # exit(0)

    barchart = BarchartDownloader()
    barchart.download()
    print(barchart.settlement_df)  # Actual data. No need to invoke download()
    for commodity in ("CO2", "FX", "Crypto", "Stock"):
        # Plots evolution of settlement prices and adjusted settlement prices for selected market
        try:
            barchart.settle_xs(commodity=commodity, offset=1).plot()
        except Exception as e:
            barchart.settle_xs(commodity=commodity, offset=0).plot()
        plt.show()
        # mpld3.show(port=mpld3_port)

"""
Sample for downloading data and do some plots
"""
from commodity_data.downloaders import OmipDownloader, BarchartDownloader
import matplotlib.pyplot as plt
import mpld3

if __name__ == '__main__':
    mpld3_port = 9999
    omip = OmipDownloader()
    omip.download("2020-01-01")
    omip = OmipDownloader()
    print(omip.settlement_df)  # Actual data. No need to invoke download()

    for product in "YMQD":
        omip.settle_xs(market="Omip", commodity="Power", area="ES", product=product, offset=slice(1, 2)).plot()
        plt.title(product)
        plt.show()

    barchart = BarchartDownloader()
    barchart.download()
    print(barchart.settlement_df)  # Actual data. No need to invoke download()
    for commodity in ("CO2", "FX", "Crypto", "Stock"):
        # Plots evolution of settlement prices and adjusted settlement prices for selected market
        try:
            barchart.settle_xs(commodity=commodity, offset=1).plot()
        except:
            barchart.settle_xs(commodity=commodity, offset=0).plot()
        plt.show()
        # mpld3.show(port=mpld3_port)

"""
Tells how to delete data from database. This should not be done, so it is very manual, it is not included in the
main downloader
"""

from commodity_data.downloaders import OmipDownloader

omip = OmipDownloader(roll_expirations=False)

bad_date = "2024-03-27"
omip.delete_dates(bad_date, bad_date)
omip.download(bad_date, bad_date, force_download=True)
df = omip.settle_xs(product="Y", offset=[1, 2], area="ES", commodity="Power")
print(df[bad_date:bad_date])

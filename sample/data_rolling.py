"""
Examples on how to perform price rolling
---
Expirations are automatically rolled unless  added roll_expiration=True to CommodityData constructor
Rolling expirations means building values for columns "adj_close" in level "type", that contain continuous prices
for products. Continuous prices mean that when product expires, price is adjusted to price of product with following
offset but adjusted to product differences to avoid introducing steps in data.
Continuous prices can be used for inputs to algo trading processes directly. Assumes that the volume is constant
but *include no bid-offer spread on rolling*!
"""

from commodity_data import CommodityData

cdty = CommodityData()

#######################
# Rolling expirations.
#######################
# Force rolling expirations two days before expiration
cdty.roll_expiration(roll_offset=2)
# Force rolling expirations at expiration day
cdty.roll_expiration()

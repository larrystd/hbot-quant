#include <cassert>
#include <string_view>

#include "hquant/venue/binance/auth.h"

int main() {
  using hquant::v1::BinanceDecimal;
  using hquant::v1::BinanceHmacSignature;

  // Published Binance Spot SIGNED order example:
  // https://developers.binance.com/en/docs/products/spot/rest-api
  constexpr std::string_view secret =
      "NhqPtmdSJYdKjVHjA7PZj4Mge3R5YNiP1e3UZjInClVN65XAbvqqM6A7H5fATj0j";
  constexpr std::string_view payload =
      "symbol=LTCBTC&side=BUY&type=LIMIT&timeInForce=GTC&quantity=1"
      "&price=0.1&recvWindow=5000&timestamp=1499827319559";
  assert(BinanceHmacSignature(secret, payload) ==
         "c8db56825ae71d6d79447849e617115f4a920fa2acdcab2b053c4b2838bd6b71");
  assert(BinanceDecimal(0) == "0");
  assert(BinanceDecimal(100'000'000) == "0.1");
  assert(BinanceDecimal(1'000'000'001) == "1.000000001");
}

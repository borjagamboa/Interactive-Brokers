from ibapi.wrapper import EWrapper
from ibapi.client import EClient
from ibapi.contract import Contract
import time, threading
import pandas as pd

class IBapi(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.cripto_historical = []

    def tickPrice(self, reqId, tickType, price, attrib):
        if tickType == 2 and reqId ==1:
            print('The current ask price is: ', price)

    def historicalData(self, reqId, bar):
        print(f'Time: {bar.date} Open: {bar.open} High: {bar.high}'
              f' Low: {bar.low} Close: {bar.close} ')
        self.cripto_historical.append([bar.date, bar.open, bar.high, bar.low, bar.close])

    def historicalDataEnd(self, reqId:int, start:str, end:str):
        """ Marks the ending of the historical bars reception. """
        print('Historical data loaded')

    def get_cripto_historical(self, cripto_ticker, duration:str, barsize:str):
        '''
        Returns cripto historical data in Pandas DataFrame
        '''
        cripto_contract = Contract()
        cripto_contract.symbol = cripto_ticker
        cripto_contract.secType = "CRYPTO"
        cripto_contract.exchange = "PAXOS" # This the exchange for all criptos in IB
        cripto_contract.currency = "USD"

        # Request Market Data for last 50 days in hourly bar-size
        self.reqHistoricalData(1, cripto_contract, '', duration, barsize,
                              'AGGTRADES', 0, 2, False, [])

        time.sleep(5)  # Sleep interval to allow time for incoming price data

        # Store data in Pandas DataFrame
        df = pd.DataFrame(self.cripto_historical, columns=['DateTime', 'Open', 'High', 'Low', 'Close'])
        df['DateTime'] = pd.to_datetime(df['DateTime'], unit='s')
        df.set_index(['DateTime'], inplace=True)
        return df


if __name__ == "__main__":
    def run_loop():
        app.run()


    app = IBapi()
    app.connect("127.0.0.1", 7497, clientId=123)

    # Start the socket in a thread
    api_thread = threading.Thread(target=run_loop, daemon=True)
    api_thread.start()
    time.sleep(1) # Sleep interval to allow time for connection to server
    print("serverVersion:%s connectionTime:%s" % (app.serverVersion(), app.twsConnectionTime()))

    df = app.get_cripto_historical("BTC", duration='100 D', barsize='1 hour')
    app.disconnect()
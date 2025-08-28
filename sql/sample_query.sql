SELECT ID, TradeAccountID, Ticket, SymbolName, Digits, Type, Quantity, State, OpenTime, OpenPrice, OpenRate, CloseTime, ClosePrice, CloseRate, StopLoss, TakeProfit, Expiration, Commission, CommissionAgent, Swap, Profit, Tax, Magic, Comment, TimeStamp
FROM mt4_pamm_debug.tradehistories th
WHERE th.TradeAccountID IN (
111, 222, 333
);

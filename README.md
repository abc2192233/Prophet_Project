# Prophet Project

#### 注意事项

Prophet在更新拟合时，即读取预训练模型时，必须要实现规定好 yearly_seasonality/weekly_seasonality/daily_seasonality 的参数值，不能为空，默认参数值为auto，如果在预训练模型生成时和更新拟合时不同时定义相同的拟合参数，会导致模型的beta参数不一致问题

相对的，在生成和更新拟合模型时同时也要加入n_changepoints=X(X为趋势变化点数量)的参数，保证delta数据维度的一致性
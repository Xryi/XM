import akshare as ak
import pandas as pd

# from Common.parallel_computing import MultiProcess
#

class a():
    global stock_to_ind
    stock_to_ind = {}
# 	pass
# A = a()


def load_x(data):
    ind,gainian_dict,hangye_dict= data[0], data[1], data[2]
    try:


        stock_board_concept_cons_ths_df = ak.stock_board_cons_ths(symbol=ind)

        stock_board_concept_cons_ths_df = stock_board_concept_cons_ths_df.query("'ST' not in @stock_board_concept_cons_ths_df.名称")
        stock_listi = [i + '.SH' if i[0] == '6' else i + '.SZ' for i in list(stock_board_concept_cons_ths_df.代码)]
        if len(stock_listi)==1:
            pass
        else:
            if ind in gainian_dict:
                # print(gainian_dict[ind], len(stock_listi), stock_listi)
                for i in stock_listi:
                    if i in stock_to_ind:
                        stock_to_ind[i].append(gainian_dict[ind])
                    else:
                        stock_to_ind[i] = [gainian_dict[ind]]
                # print(stock_to_ind)
                return [gainian_dict[ind],len(stock_listi),stock_listi]

            elif ind in hangye_dict:
                # print(hangye_dict[ind], len(stock_listi), stock_listi)
                for i in stock_listi:
                    if i in stock_to_ind:
                        stock_to_ind[i].append(hangye_dict[ind])
                    else:
                        stock_to_ind[i] = [hangye_dict[ind]]
                # print(stock_to_ind)
                return [hangye_dict[ind],len(stock_listi),stock_listi]

    except Exception as e:
        print(e,ind)




if __name__ == '__main__':
    stock_board_concept_name_ths_df = ak.stock_board_concept_name_ths()
    stock_board_industry_summary_ths_df = ak.stock_board_industry_name_ths()

    gainian_dict = {}
    hangye_dict = {}

    all_ind_list = []

    for i in range(len(stock_board_industry_summary_ths_df)):

        hangye_dict[stock_board_industry_summary_ths_df.code.iloc[i]] = stock_board_industry_summary_ths_df.name.iloc[i]
        all_ind_list.append(stock_board_industry_summary_ths_df.code.iloc[i])



    for i in range(len(stock_board_concept_name_ths_df)):
        if stock_board_concept_name_ths_df['概念名称'].iloc[i] not in ['融资融券', '深股通', '沪股通', '标普道琼斯A股', '专精特新', 'MSCI概念', '年报预增',
                       '人民币贬值受益', '国企改革', '创投', '新股与次新股', '高股息精选', '信创', '粤港澳大湾区', \
                       '注册制次新股', '证金持股', '参股新三板', '长三角一体化', '福建自贸区', '参股券商', '杭州亚运会',
                       '同花顺中特估100', '同花顺漂亮100', '京津冀一体化', '参股保险', '分拆上市意愿',
                       '核准制次新股', '送转填权','雄安新区','首发新股','ST板块']:

            gainian_dict[stock_board_concept_name_ths_df['代码'].iloc[i]] = stock_board_concept_name_ths_df['概念名称'].iloc[i]
            all_ind_list.append(stock_board_concept_name_ths_df['代码'].iloc[i])



    data_into = [[i,gainian_dict,hangye_dict] for i in all_ind_list]
    result = []
    for data_intoi in data_into:
        result.append(load_x(data_intoi))
        with open(file='C:\光大证券金阳光QMT实盘\mpython\ind_update.txt',mode='a',encoding='utf') as lf:
            lf.write(data_intoi[0])
            lf.write("\n")

    # result = MultiProcess.multi_process(load_x,data_into,max_workers=1)
    result = [i for i in result if i]
    df = pd.DataFrame(result)
    df.columns = ['ind','num','stock_list']
    # df['ind']= df.ind.apply(lambda x:gainian_dict[x] if x in gainian_dict else hangye_dict[x])
    # df['stock_list'] = df['stock_list'].apply(lambda x: [i+'.SH' if i[0]=='6' else i+'.SZ' for i in x])

    df.to_excel('C:/光大证券金阳光QMT实盘/python/xry_data/ind_tsh_aks2.xlsx', index=False)
    pass

    stock_ind_ = []
    for sti in stock_to_ind:
        stock_ind_.append([sti,stock_to_ind[sti]])
    df_stock_ind = pd.DataFrame(stock_ind_)

    df_stock_ind.columns = ['股票代码','所属概念']
    df_stock_ind.to_excel('C:/光大证券金阳光QMT实盘/python/xry_data/ind_tsh_aks3.xlsx', index=False)
    # df = pd.read_excel('D:光大证券金阳光QMT实盘/python/qmt_data/ind_tsh_aks2.xlsx')
    # all_stock = []
    # for i in range(len(df)):
    #     all_stock+=df.stock_list.iloc[i]
    # stock_set = set(all_stock)
    #
    # stock_to_ind = {}
    # for stock in stock_set:



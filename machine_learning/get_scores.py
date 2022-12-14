import pandas as pd
import pickle
import os
from datetime import datetime

PATH_PICKLE = os.path.join("machine_learning", "models", "{}")
PATH_DATA   = os.path.join("data", "{}")


modelos = ['SVM','Random Forest','AdaBoost','Gradient Boosting','LGBM']

def create_rank(results, n_rank):
    rank = pd.DataFrame()
    for modelo in modelos:
        try:
            temp = results.sort_values(by='PREDICTED MVP SHARE '+modelo, ascending=False)[:n_rank].reset_index(drop=True)
            rank['MVP RANK '+modelo] = temp['PLAYER']
            rank['MVP SHARE '+modelo] = round(temp['PREDICTED MVP SHARE '+modelo],3)
        except:
            continue
    
    return rank

#############################################

def get_scores():

    # Abrindo base
    today = datetime.today().strftime('%d_%m_%y')

    df = pd.read_parquet(
        PATH_DATA.format(
            f'{today}.parquet'
        ),
    )

    # fix types
    df["PER_ADVANCED"] = df["PER_ADVANCED"].apply(pd.to_numeric)

    # Filtrando jogadores
    df = df[((df['PTS_PERGAME']>13.5)&(df['MP_PERGAME']>30)
            &(df['SEED']<=16)&(df['AST_PERGAME']>1)&(df['TRB_PERGAME']>3)
            &(df['FG%']>0.37)&(df['FGA_PERGAME']>10)
            &(df['PER_ADVANCED']>18))].reset_index(drop=True)

    # Projeção de vars. totais
    df['G_LEFT'] = 82 - df['G_TEAM']
    df['MULT_G'] = df['G_LEFT']/df['G']+1

    cols_total = [x for x in df.columns if x.endswith('_TOTAL')]
    for col in cols_total:
        df[col] = round(df[col] * df['MULT_G'],0)

    # Alterando tipos p/int
    int_cols = [x for x in df.columns if x.endswith('_TOTAL')]
    int_cols.extend(['AGE','G','GS','EXPERIENCE','COLLEGE','NATIONALITY_US','SEED'])
    df[int_cols] = (df[int_cols]).astype(int)

    # Projeção de vars. avançadas
    vars_to_proj = ['OWS_ADVANCED','DWS_ADVANCED','WS_ADVANCED','VORP_ADVANCED']
    for col in vars_to_proj:
        df[col] = round(df[col] * df['MULT_G'],1)
        
    features = pickle.load(open(PATH_PICKLE.format("features.dat"),'rb'))
    X = df[features]

    scaler = pickle.load(open(PATH_PICKLE.format('standard_scaler.dat'),'rb'))
    scaled_X = scaler.transform(X)

    initial_results = df[['PLAYER']]
    results = initial_results.copy()

    # Prevendo MVP Share p/cada modelo
    for modelo in modelos:
        try:
            model = pickle.load(open(PATH_PICKLE.format(f"{modelo}.dat"),'rb'))
                
            y_pred = model.predict(scaled_X)

            apoio = initial_results.copy()
            apoio['PREDICTED MVP SHARE '+modelo] = pd.Series(y_pred).values

            results_sorted = apoio.sort_values(by='PREDICTED MVP SHARE '+modelo,
                                                ascending=False).reset_index(drop=True)
            results_sorted['MVP RANK '+modelo] = results_sorted.index+1

            results = results.merge(results_sorted, on=['PLAYER'])
        except:
            continue


    rank = create_rank(results, 10)

    # Aplicando critérios p/definir rank final
    rank_columns = [x for x in rank.columns if x.startswith('MVP RANK ')]
    rank_t = rank[rank_columns].T
    
    rank_f = []

    for i in range(0,10):
        # 1º Critério: maioria dentre os 5 modelos
        n_vzs = rank_t[i].value_counts().sort_values(ascending=False)[0]
        player = rank_t[i].value_counts().sort_values(ascending=False).index[0]
        
        k = 1
        while player in rank_f:
            n_vzs = rank_t[i].value_counts().sort_values(ascending=False)[k]
            player = rank_t[i].value_counts().sort_values(ascending=False).index[k]
            k+=1
        
        if n_vzs >= 3:
            rank_f.append(player)
            
        # 2º Critério: maioria dentre SVM, Gradient Boosting e LGBM
        else:
            rank_2 = rank_t.drop(['MVP RANK Random Forest','MVP RANK AdaBoost'], axis=0)
            
            n_vzs = rank_2[i].value_counts().sort_values(ascending=False)[0]
            player = rank_2[i].value_counts().sort_values(ascending=False).index[0]
            
            j = 1
            while player in rank_f:
                n_vzs = rank_2[i].value_counts().sort_values(ascending=False)[j]
                player = rank_2[i].value_counts().sort_values(ascending=False).index[j]
                j+=1
            
            if n_vzs >= 2:
                rank_f.append(player)
                
            # 3º Critério: soma das MVP Shares
            else:
                players = rank_t[i].value_counts().sort_values(ascending=False).index.to_list()
                
                players = [x for x in players if x not in rank_f]
                
                mvp_share = pd.DataFrame(columns=['MVP SHARE'],index=players)
                for player in players:
                    sum_mvp_s = 0
                    for model in modelos:
                        apoio = rank[['MVP RANK '+model,'MVP SHARE '+model]]
                        mvp_s = apoio[apoio['MVP RANK '+model]==player]['MVP SHARE '+model].sum()
                        sum_mvp_s = sum_mvp_s + mvp_s
                    mvp_share['MVP SHARE'][mvp_share.index==player] = sum_mvp_s
                    
                rank_f.append(mvp_share.sort_values(by='MVP SHARE',ascending=False).index[0])
            
    rank['MVP RANK FINAL'] = rank_f

    rank.to_csv(
        path_or_buf=os.path.join("machine_learning", "predictions", f"rank_{today}.csv")
    )

    # Gerar df do rank final c/stats
    get_final_rank(rank, df)

    # Gerar df com evolução do rank final
    # get_evolution()
    
    # Gerar gráficos SHAP
    # get_shap(X, scaled_X)

    return rank

# #############################################

def get_final_rank(rank, df):

    rank['Predicted MVP Rank'] = rank.index+1
    rank = rank.rename(columns={'MVP RANK FINAL':'PLAYER'})
    rank2 = rank[['Predicted MVP Rank','PLAYER']].merge(df,on='PLAYER',how='left',validate='1:1')

    rank2 = rank2[['Predicted MVP Rank',"PLAYER",'POS',"TEAM","AGE","HEIGHT",'G','MP_PERGAME','PTS_PERGAME','TRB_PERGAME',
                   'AST_PERGAME','STL_PERGAME','BLK_PERGAME','FG%','3P%','FT%','PER_ADVANCED','WS/48_ADVANCED','VORP_ADVANCED',
                   'PCT','SEED']]
    
    rank2['HEIGHT'] = round(rank2['HEIGHT'],1)
    rank2['PCT'] = round(rank2['PCT'],3)
    rank2['WS/48_ADVANCED'] = round(rank2['WS/48_ADVANCED'],3)
    rank2[['FG%','3P%','FT%']] = round(rank2[['FG%','3P%','FT%']]*100,2)

    rank2 = rank2.rename(columns={'PLAYER':"Player",'POS':'Position','TEAM':"Team",'AGE':"Age",
                                  'HEIGHT':"Height (cm)",'G':'Games','MP_PERGAME':'Minutes',
                                  'PTS_PERGAME':'PTS','TRB_PERGAME':'REB','AST_PERGAME':'AST',
                                  'STL_PERGAME':'STL','BLK_PERGAME':'BLK','PER_ADVANCED':'PER',
                                  'WS/48_ADVANCED':'WS/48','VORP_ADVANCED':'VORP (Projected)',
                                  'PCT':'PCT Team','SEED':'Seed'})

    rank2.to_csv(
        path_or_buf=os.path.join("machine_learning", "predictions", f"MVP_Rank_Stats.csv"),
        sep=',',
        decimal='.',
        index=False
    )

# #############################################

# def get_evolution():
#     arquivos = os.listdir(PATH_SAVE)

#     evolution = pd.DataFrame()
#     for arquivo in arquivos:
#         if arquivo.startswith('MVP_Rank_') and arquivo.startswith('MVP_Rank_Stats')==False:
#             apoio = pd.read_csv(path_data+sep+arquivo,
#                                  sep=',', decimal='.')
            
#             data = arquivo[9:19]
#             apoio['Date'] = pd.to_datetime(data, format='%d_%m_%Y').strftime('%d/%m/%Y')
            
#             apoio['Rank'] = apoio.index + 1
            
#             apoio = apoio.rename(columns={'MVP RANK FINAL':'Predicted MVP Rank'})
            
#             apoio = apoio[['Rank','Predicted MVP Rank','Date']]
            
#             evolution = pd.concat([evolution,apoio],ignore_index=True)
            
#     evolution.to_csv(path_data+sep+'Evolution.csv',
#                     sep=',', decimal='.',index=False)

# #############################################

# def get_shap(X, scaled_X):
#     modelos = ['SVM','Random Forest','AdaBoost','Gradient Boosting','LGBM']
    
#     for model in modelos:
#         model = pickle.load(open(os.path.join(path_pickle,model+'.dat'),'rb'))

#         explainer = shap.Explainer(model.predict, scaled_X)
#         shap_values = explainer(scaled_X)

#         shap.summary_plot(shap_values, X, plot_type='violin', show=False)
#         plt.savefig(path_data+sep+model+'_SHAP.png', format='png', dpi=700, bbox_inches='tight')


rank = get_scores()
print(rank)
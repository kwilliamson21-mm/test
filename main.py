import mmpac
import pandas as pd
import argparse
from datetime import date
from pathlib import Path
import numpy as np

# run the below from terminal
'''
python .\lifeclaims-optimalcaseload\main.py --v_batch="batch_id" --v_pwd="password" --v_env_source="sc2_prod"
'''

parser = argparse.ArgumentParser()

parser.add_argument("--v_batch")
parser.add_argument("--v_pwd")
parser.add_argument("--v_env_source")
parser.add_argument("--v_env_target")

args = parser.parse_args()

v_batch = args.v_batch
v_pwd = args.v_pwd
v_env_source = args.v_env_source
v_env_target = args.v_env_target


def transform_examiner_data(data) -> dict:
    fields = {'short_dt': 'datetime64[ns]',
              'fact_integrated_natural_key_hash_uuid': 'object',
              'dim_employee_natural_key_hash_uuid': 'object',
              'party_employee_id': 'int64',
              'employee_last_nm': 'object',
              'all_day_ooo': 'int64',
              'complexity_level_limit': 'int64',
              'prod_credits_available': 'float64'}

    test_fields = ['working_hours',
                   'admin_time',
                   'planned_non_prod_hrs',
                   'planned_prod_hrs',
                   'planned_excused_hrs',
                   'planned_ooo_hrs',
                   'planned_ot_hrs']

    party_employee_id = data.loc[:, 'party_employee_id'].to_dict()
    test_fields = data.loc[:, test_fields].to_dict()

    exceptions = []
    for key in party_employee_id:
        for field in test_fields:
            if test_fields[field][key] < 0:
                exceptions.append(party_employee_id[key])
                break

    if len(exceptions) == 0:
        df = data.loc[data['role_id'].isin([15, 16])]
    else:
        df = data.loc[data['role_id'].isin([15, 16]) &
                      (~data['party_employee_id'].isin(exceptions))]

    df = df.loc[:, fields.keys()]
    df = df.astype(fields)
    df.reset_index(drop=True, inplace=True)

    result = {'map': df.iloc[:, 0:4], 'examiners': df.iloc[:, 3:]}
    return result


def transform_pending_inventory_data(data) -> dict:
    fields = {'ref_wrk_ident_natural_key_hash_uuid': 'object',
              'source_transaction_id': 'int64',
              'party_employee_id': 'int64',
              'work_event_nm': 'object',
              'days_past_tat': 'int64',
              'prod_credit': 'float64',
              'fk_rsrcusr_ident': 'object',
              'assigned': 'bool',
              'target_work_event': 'bool'}

    df = data.loc[:, fields.keys()]
    df['party_employee_id'] = df['party_employee_id'].fillna(-99)
    df = df.astype(fields)

    result = {'map': df.iloc[:, 0:2], 'pending': df.iloc[:, 1:]}
    return result


def calculate_load(work) -> pd.DataFrame:
    result = work.loc[:, ['party_employee_id', 'source_transaction_id', 'prod_credit']].groupby(
        by=['party_employee_id'], as_index=False).agg({'source_transaction_id': 'count', 'prod_credit': 'sum'})
    result.rename(columns={'source_transaction_id': 'items',
                  'prod_credit': 'prod_credits'}, inplace=True)

    return result


def derive_unassigned_complexity_level(work) -> pd.DataFrame:
    result = work.copy()
    result['wrk_level'] = result['fk_rsrcusr_ident'].str[:2]
    result['lvl'] = (result['wrk_level'].str[1:]).astype('int64')
    return result


def assignment_algorithm(workers, work) -> dict:
    workers = workers.copy()
    print(workers)
    assignments = []
    assignment_columns = ['source_transaction_id',
                          'party_employee_id', 'fk_rsrcusr_ident', 'prod_credit']
    no_assignments = []
    no_assignment_columns = ['source_transaction_id', 'party_employee_id']

    print('Below is the work')
    print(work, end='\n')

    print('Starting work item loop...')
    for item in work.itertuples():
        print('Assessing the item below')
        print(work.loc[[item.Index]], end='\n')
        candidates = workers.loc[(workers['complexity_level_limit'] - item.lvl >= 0) &
                                 (workers['capacity'] - item.prod_credit >= 0)]
        num_candidates = len(candidates.index)
        if num_candidates > 0:
            max_capacity = candidates['capacity'].agg('max')
            worker = workers.loc[workers['capacity'] == max_capacity]
            print('There are examiners with capacity')
            print('Below are examiners that have maximum capacity value')
            print(worker)
            num_workers = len(worker.index)
            if num_workers == 1:
                index_num = worker.index.to_list().pop()
            else:
                row_num = np.random.randint(num_workers)
                print('Generate a random number to randomly select an examiner')  # nopep8
                print(row_num)
                worker = worker.iloc[[row_num]]
                print('Below is selected examiner')
                print(worker)
                index_num = worker.index.to_list().pop()
            # print('Below is their index number')
            # print(target)
            print('Below is the row selection based on the index number')
            print(workers.loc[[index_num]])
            workers.at[index_num, 'capacity'] = max_capacity - item.prod_credit
            # workers.at[index_num, 'pending_items'] += 1
            print(max_capacity - item.prod_credit)
            print(workers.loc[[index_num]])
            data = []
            data.append(item.source_transaction_id)
            data.append(workers.at[index_num, 'party_employee_id'])
            data.append(item.fk_rsrcusr_ident)
            data.append(item.prod_credit)
            print(data)
            assignments.append(data)
        else:
            data = []
            data.append(item.source_transaction_id)
            data.append(item.party_employee_id)
            data.append(item.fk_rsrcusr_ident)
            data.append(item.prod_credit)
            no_assignments.append(data)
    assignments = pd.DataFrame(assignments, columns=assignment_columns)
    no_assignments = pd.DataFrame(no_assignments, columns=assignment_columns)
    print(assignments)
    print(no_assignments)
    return {'assignments': assignments, 'no_assignments': no_assignments}


mmpac.vertica_setup(server=v_env_source, user=v_batch, password=v_pwd)

p = Path()

# add treatment if dataframes are empty, as in query executes but no records returned

f = p / 'lifeclaims-optimalcaseload' / 'TimeOutData3.sql'

examiners = transform_examiner_data(data=mmpac.get_query(f.open().read()))
examiners_map = examiners['map']
examiners = examiners['examiners']
print(examiners)
f = p / 'lifeclaims-optimalcaseload' / 'PendingInventory3.sql'

# pending = transform_pending_inventory_data(data=mmpac.get_query(f.open().read()))  # nopep8
pending = transform_pending_inventory_data(
    pd.read_csv(p/'lifeclaims-optimalcaseload'/'testdata.csv'))
pending_map = pending['map']
pending = pending['pending']

assigned = pending.loc[(pending['assigned'] == True) &
                       (pending['target_work_event'] == True) &
                       (pending['days_past_tat'] >= 0)]

load = calculate_load(assigned)
load.rename(columns={'items': 'assigned_items',
                     'prod_credits': 'assigned_prod_credits'}, inplace=True)
examiners = examiners.merge(load, on=['party_employee_id'], how='left').fillna(0)  # nopep8


examiners['capacity'] = examiners['prod_credits_available'] - examiners['assigned_prod_credits']  # nopep8


test = assigned.merge(examiners,  on=['party_employee_id'])
test = test.loc[test['all_day_ooo'] > 0]
print(test)

unassigned = pending.loc[(pending['assigned'] == False) &
                         (pending['target_work_event'] == True) &
                         (pending['days_past_tat'] >= 0)]

unassigned = derive_unassigned_complexity_level(unassigned)

unassigned.sort_values(by=['wrk_level', 'days_past_tat', 'prod_credit'],
                       ascending=[False, False, False], inplace=True)

assignments = assignment_algorithm(examiners, unassigned)
no_assignments = assignments['no_assignments']
assignments = assignments['assignments']

load = calculate_load(assignments)
load.rename(columns={'items': 'assignment_items',
                     'prod_credits': 'assignment_prod_credits'}, inplace=True)
examiners = examiners.merge(load, on='party_employee_id', how='left').fillna(0)
print(examiners)
print(load)
print(assignments)
print('no assignments')
print(no_assignments)
# print(no_assignments)
# test_work = pd.concat([assignments, no_assignments])


print(assignments.dtypes)
assignments = pending_map.merge(assignments, on='source_transaction_id')
print(assignments)
assignments['prod_credit'] = assignments['prod_credit'].astype('int64')
print(assignments)
print(examiners)

# uncomment to test load, but change table name
# if mmpac.has_table('kw_lc_caseload_test', 'dma_analytics'):
#    mmpac.bulk_load(assignments, 'dma_analytics', 'kw_lc_caseload_test')
# else:
#    mmpac.send_update(
#        sql="CREATE TABLE dma_analytics.kw_lc_caseload_test (source_transaction_id INT, party_employee_id INT, fk_rsrcusr_ident VARCHAR(7), prod_credit INT);")
#    mmpac.bulk_load(assignments, 'dma_analytics', 'kw_lc_caseload_test')


mmpac.vertica_disconnect()

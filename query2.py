import pykx as kx

def leftMerge(left, right, lefton, righton):
    left = kx.q.xkey(lefton, left)
    right = kx.q.xkey(righton, right)
    left = kx.q.lj(left, right)
    return kx.q.unkey(0,left)

def main():
    # Initialize database connection
    db = kx.DB(path='/data/kdb')
    
    # Step 1: Get tools matching pattern (rfab_ds.equip) Should be ok
    et = db.pg_ees_tool.select(columns=['name', 'id'], where=[kx.Column('name').like('IMV2*')])
    
    # Step 2: Filter by equip_process name = 'DataCollector' (rfab_ds.equip_process)
    # Assuming this filter is already applied in the tool lookup or proc_type_id represents this
    etc = db.pg_ees_equip_templ_chamber_def.select(columns=['name','id','proc_type_id'], where=[kx.Column('id').isin(et['id']), kx.Column('name').like('DataCollector')])
    ep = db.equip_signal_persisted.select(columns= ["id","signal_id"],where=[kx.Column('equip_id').isin(et['id'])])
    
    # Step 3: Get sensors with pressure in name (rfab_ds.signal)
    s = db.ees_sensor_lookup.select(
        columns=['id', 'name', 'proc_type_id'],
        where=[
            kx.Column('proc_type_id').isin(etc['proc_type_id']),
            kx.Column('id').isin(ep['signal_id']),
            kx.Column('name').lower().like('*pressure*')
            ])
    
    # Step 4: Get process runs with recipe filter (rfab_ds.process_run)
    #r = db.pg_ees_process_type.select(columns=['id', 'start_time', 'end_time', 'recipe'],where=[kx.Column('id').isin(et['id']),kx.Column('recipe').lower().like('*process.qa*')])
    r = db.ees_run_context.select(
        columns=['tool_id','tr_id','run_id','start_time','ctx_value'],
        where=[
            kx.Column('tool_id').isin(et['id']),
            kx.Column('ctx_value').lower().like('*process.qa*')
            ]) 
    # iterate here. get the starttimes for each day.

    dates = kx.q('date')
    counter = 0
    for i in dates:
        r = db.ees_run_context.select(
            columns=['tool_id','tr_id','run_id','start_time','ctx_value'],
            where=[
                kx.Column('date').isin(i),
                kx.Column('tool_id').isin(et['id']),
                kx.Column('ctx_value').lower().like('*process.qa*')
                ]) 
        if r.shape[0] == 0:
            continue
        else:
            er = db.ees_run.select(columns=[kx.Column('end_time').max(),kx.Column('start_time').min()],where=[ kx.Column('date').isin(i), kx.Column('tr_id').isin(r['tr_id'])])
            start_time = er['start_time'].min()
            end_time = er['end_time'].max()
            sd = db.ees_sensor_data.select(columns=['tool_id','ts_id, data_float'], where=[kx.Column('date')==i, kx.Column('ts_id').isin(ep['id']), kx.Column['min_time'] >= start_time, kx.Column['max_time']<= end_time])
            sd = leftMerge(sd,r,'tool_id','tool_id')
            if counter == 0:
                df = sd.copy()
            else:
                df = kx.q.uj(df,sd)
            counter += sd.shape[0]
            print(counter)
            if counter >= limit:
                print(f'{limit} records reached. ending loop')
                break



    df = leftMerge(df, ep, 'ts_id', 'id')
    df = leftMerge(df, et, 'equip_id','id')
    
    return df

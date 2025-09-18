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
        sd = db.ees_sensor_data.select(columns=['tool_id','ts_id, data_float'], where=[kx.Column('date')==i, kx.Column('ts_id').isin(ep['id']), kx.Column('min_time') >= start_time, kx.Column('max_time')<= end_time])
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

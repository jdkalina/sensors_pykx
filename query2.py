import pykx as kx

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
    s = db.ees_sensor_lookup.select(columns=['id', 'name', 'proc_type_id'],where=[kx.Column('proc_type_id').isin(etc['proc_type_id']),kx.Column('id').isin(ep['signal_id']),kx.Column('name').lower().like('*pressure*')])
    
    # Step 4: Get process runs with recipe filter (rfab_ds.process_run)
    #r = db.pg_ees_process_type.select(columns=['id', 'start_time', 'end_time', 'recipe'],where=[kx.Column('id').isin(et['id']),kx.Column('recipe').lower().like('*process.qa*')])
    r = db.ees_run_context.select(columns=['tool_id','tr_id','run_id','start_time','ctx_value'],where=[kx.Column('tool_id').isin(et['id']),kx.Column('ctx_value').lower().like('*process.qa*')]) 
    # iterate here. get the starttimes for each day.
    
    #end time
    er = db.ees_run.select(columns=kx.Column('end_time').max(),where=[kx.Column('date').isin(r['start_time'].date ), kx.Column('tr_id').isin(r['tr_id'])])
    
    #Merge here, get end_time
    
    start_time = r['start_time'].min()
    start_date = start_time.date
    end_time = er['end_time'].max()
    end_date = end_time.date
        
    #iterate over days to be safe. stop at 100,000 records
		sd = db.ees_sensor_data.select(columns=['ts_id, data_float'], where=[kx.Column('ts_id').isin(ep['id'])])    

    def leftMerge(left, right, lefton, righton):
        left = kx.q.xkey(lefton, left)
        right = kx.q.xkey(righton, right)
        left = kx.q.lj(left, right)
        return kx.q.unkey(0,left)

    sd = leftMerge(sd, ep, 'ts_id', 'id')
    sd = leftMerge(sd, et, 'equip_id','id')
    sd = leftMerge(sd, r, 'equip_id','equip_id')
    sd = leftMerge(sd, s, 'signal_id','id')
    
    return sd

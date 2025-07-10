import pykx as kx

db=kx.DB(path='database')

def kx_dt(text): 
    return kx.q(f'`date${text}')

def get_runs(tool_ids,start_time,end_time):
    """
    tool_ids: accepts either a single pykx.IntAtom type or a list of kx.IntAtom
    start_time: accepts a pykx.DateAtom type
    end_time: accepts a pykx.DateAtom type
    """
    columns = ['tool_id','run_id','run_type','start_time','end_time','parent_run_id','parent_start_time']
    r= db.ees_run.select(
        columns = columns,
        where = [
            kx.Column('date').within(start_time, end_time),
            kx.Column('tool_id').isin(tool_ids),
            kx.Column('start_time') >= start_time,
            kx.Column('start_time') <= end_time  # Corrected from >= to <=
        ]
        )
    r = kx.q.xkey('tool_id', r)
    t= db.ees_tool_lookup.select(columns=['tool_id','tool_name'], 
                                 where = kx.Column('tool_id').isin(tool_ids))
    t = kx.q.xkey('tool_id', t)
    return kx.q.lj(r,t)

def get_contexts(tool_ids, context_ids, start_time, end_time):
    tinfo = db.ees_tool_lookup.select(columns=['tool_id','tool_name','proc_type_id'], 
                                      where=kx.Column('tool_id').isin(tool_ids))
    cinfo = db.ees_sensor_lookup.select(columns=[kx.Column('id').name('ctx_id'),
                                                 kx.Column('name').name('context_name'),
                                                 kx.Column('data_type')],
                                        where=[kx.Column('proc_type_id').isin(tinfo['proc_type_id']),
                                               kx.Column('id').isin(context_ids)])
    cmap = kx.q.xkey('ctx_id', cinfo)
    r = db.ees_run_context.select(columns= [kx.Column('tool_id'), 
                                            kx.Column('run_id'),
                                            kx.Column('ctx_id').name('context_name'),
                                            kx.Column('ctx_value')],
                                            where = [kx.Column('date').within(start_time, end_time),
                                                     kx.Column('tool_id').isin(tool_ids),
                                                     kx.Column('ctx_id').isin(context_ids)]
                                            )
    r['context_name'] = cmap[r['context_name']]['context_name']
    r = kx.q.xkey('tool_id', r)
    t = kx.q.xkey('tool_id', tinfo)
    return kx.q.lj(r,t)

def get_sensor_data(tool_id, run_id, start_time, end_time):
    """
    datatype: 'float' or 'int' or 'str'
    tool_id: kx integer type
    run_id: kx integer type
    start_time: KX datetime
    end_time: KX datetime
    """
    tinfo = db.ees_tool_lookup.select(columns=['tool_id','tool_name','proc_type_id'], where=kx.Column('tool_id').isin(tool_id))
    tmap = kx.q.xkey('tool_id', tinfo[['tool_id','tool_name']])
    sinfo = db.ees_sensor_lookup.select(columns = [(((kx.Column('id') < 0) + tool_id) * 4294967296 + kx.Column('id')).name('ts_id'),
                                                   kx.Column('id').name('sensor_id'),
                                                   kx.Column('name').name('sensor_name'),
                                                   kx.Column('data_type')],
                                                   where = kx.Column('proc_type_id').isin(tinfo['proc_type_id']))
    smap = kx.q.xkey('sensor_id', sinfo[['sensor_id','sensor_name']])
    sinfo['run_id'] = run_id
    senfloat = []
    senlong = []
    senstr = []
    if len(f := sinfo.exec("ts_id", where="data_type in `float`double")) > 0:
        senfloat = db.ees_sensor_data.select(
            columns=[
                kx.Column('tool_id').name('tool_name'), 
                kx.Column('sensor_id').name('sensor_name'),  
                kx.Column('time_stamps'),
                kx.Column('data_float')
            ],
            where=[
                kx.Column('date').within(start_time, end_time),
                kx.Column('ts_id').isin(f)]
        )
    if len(l := sinfo.exec("ts_id", where="data_type in `int")) > 0:
        senlong = db.ees_sensor_data.select(
            columns=[
                kx.Column('tool_id').name('tool_name'), 
                kx.Column('sensor_id').name('sensor_name'),  
                kx.Column('time_stamps'),
                kx.Column('data_long')
            ],
            where=[
                kx.Column('date').within(start_time, end_time),
                kx.Column('ts_id').isin(l)]
        )
    if len(s := sinfo.exec("ts_id", where="data_type in `string")) > 0:
        senstr = db.ees_sensor_data.select(
            columns=[
                kx.Column('tool_id').name('tool_name'), 
                kx.Column('sensor_id').name('sensor_name'),  
                kx.Column('time_stamps'),
                kx.Column('data_str')
            ],
            where=[
                kx.Column('date').within(start_time, end_time),
                kx.Column('ts_id').isin(s)]
        )
        senstr['data_str'] = senstr['data_str'].pd().astype(str).str.split('\x00')
    return (kx.q.ungroup(senfloat), kx.q.ungroup(senlong), kx.q.ungroup(senstr))

class samples:
    def __init__(self):
        self.start_time = kx_dt('2025.05.30')
        self.end_time = kx_dt('2025.06.01')
        self.tool_id = kx.toq(7139, ktype=kx.IntAtom)
        self.run_id = "SampleRUNID"
        self.context_ids = [2395232,2395228,2395233,2395230] 
        self.grouped = False
        self.context_ids = kx.toq([0,1,2])

if __name__ == '__main__':
    data = samples()
    get_sensor_data(datatype = 'int', tool_id = data.tool_id, run_id=data.run_id, start_time=data.start_time, end_time=data.end_time)
    get_contexts(tool_ids = data.tool_id, context_ids=data.context_ids, start_time=data.start_time, end_time=data.end_time)
    get_runs(tool_ids = data.tool_id,start_time=data.start_time,end_time=data.end_time)


    # tinfo = db.ees_tool_lookup.select(columns=['tool_id','tool_name','proc_type_id'], where=kx.Column('tool_id').isin(tool_id))
    # tmap = kx.q.xkey('tool_id', tinfo[['tool_id','tool_name']])
    # sinfo = db.ees_sensor_lookup.select(columns = [(((kx.Column('id') < 0) + tool_id) * 4294967296 + kx.Column('id')).name('ts_id'),
    #                                                 kx.Column('id').name('sensor_id'),
    #                                                 kx.Column('name').name('sensor_name'),
    #                                                 kx.Column('data_type')],
    #                                                 where = kx.Column('proc_type_id').isin(tinfo['proc_type_id']))

    # # ts_id is a synthetic id created from tool_id and sensor_id, tehse are 4byte int . shifting by 4 bytes.


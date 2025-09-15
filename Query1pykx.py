import pykx as kx

tool_name = "IMV20J*"

db = kx.DB(path='database')

def kx_dt(text):return kx.q(f'`date${text}')

def kx_time(text):return kx.q(f'`datetime${text}')

start = "2025.07.20"
end = "2025.07.21"
stdt = kx_dt(start)
enddt = kx_dt(end)
starttime = kx_time(start)
endtime = kx_time(end)

tinfo = db.ees_tool_lookup.select(columns=['tool_id','proc_type_id','tool_name'], where=[kx.Column('tool_name').like(tool_name)]) # returns back proc_type_id of 7139

sinfo = db.ees_sensor_lookup.select(columns = [kx.Column('id').name('sensor_id'),kx.Column('name'),kx.Column('proc_type_id')],where = [kx.Column('proc_type_id').isin(tinfo['proc_type_id']),kx.Column("name").like("*Pressure*")]) # returns back sensor_id 2394585

kx.q['temptable'] = kx.q.lj(kx.q.xkey('proc_type_id',tinfo),kx.q.xkey('proc_type_id',sinfo))
ts = kx.q('select name, tool_id, tool_name, proc_type_id, sensor_id, ts_id:sensor_id+4294967296*tool_id+sensor_id<0 from temptable')

cinfo = db.ees_sensor_lookup.select(columns = [kx.Column('proc_type_id'),kx.Column("id"),kx.Column("name").name('recipe')],where=[kx.Column('proc_type_id').isin(tinfo['proc_type_id']),kx.Column('name').like('Recipe')])

runctx = db.ees_run_context.select(columns=['tool_id','run_id','start_time','ctx_value'],where=[kx.Column('date').within(stdt,enddt),kx.Column('tool_id').isin(tinfo['tool_id']),kx.Column('ctx_id').isin(cinfo['id']),kx.Column('start_time').within(starttime,endtime),kx.Column('ctx_value').like(''Process.QA*')])

sd = db.ees_sensor_data.select(columns=['tool_id','sensor_id','ts_id','time_stamps','data_float'],where=[kx.Column('date').within(starttime, endtime),kx.Column('ts_id').isin(ts['ts_id']),kx.Column('min_time') >= runctx['start_time'].min()])

# Key the field to join TSID into ctx_info for recipe info
c = kx.q.xkey('proc_type_id', cinfo)
ts = kx.q.xkey('proc_type_id', ts)
df = kx.q.lj(c,ts)
df = kx.q.unkey(0,df)

# Key the field to join on SD to output
df = kx.q.xkey('ts_id',df)
sd = kx.q.xkey('ts_id', sd)
df = kx.q.lj(sd,df)

import pykx as kx

db = kx.DB(path='database')

def kx_dt(text):
	return kx.q(f'`datetime${text}')

tool = 7139
minTime = kx_dt('2025.05.30T10:00:00.000')
maxTime = kx_dt('2025.05.30T11:00:00.000')

r = db.ees_run.select(columns=['tool_id','run_id','start_time','end_time'], where=[kx.Column('tool_id').isin(tool)])

sd = db.ees_sensor_data.select(columns=['tool_id','time_stamps','data_float','data_long','data_str','sensor_id','ts_id'],where=[kx.Column('min_time').within(minTime, maxTime), kx.Column('min_time')> r['start_time'].min(), kx.Column('max_time')<= r['end_time'].max(), kx.Column('tool_id').isin(r['tool_id']) ])

tl = db.ees_tool_lookup.select(columns= ['tool_id','run_id','proc_type_id','proc_type_name'], where=[kx.Column('tool_id').isin(sd['tool_id']), kx.Column('run_id').isin(r['run_id'])])

r = kx.q.xkey('tool_id',r)
sd = kx.q.xkey('tool_id',sd)
tl = kx.q.xkey('tool_id',tl)
data = kx.q.lj(r,sd)
data = kx.q.lj(data,tl)
data = data[['time_stamps','data_float','data_long','data_str','run_id','start_time','end_time','proc_type_id','proc_type_name']]

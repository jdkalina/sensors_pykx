
# What is ees_sensor_data_array, using ees_sensor_data
# you maintain unnested timestamps and nested timestamps, do you want to continue this pattern?

#1st is eq

eq = db.equip.select(columns=[ kx.Column('id').name('tool_id'), kx.Column('equip_process_id') ],where=[ kx.Column('name').like('IMV20J*') ])


#second is eq_p to get proc_type_id

eq_p = db.equip_process.select(columns=[kx.Column('proc_type_id')],where=[kx.Column('id').isin(eq['equip_process_id'])])

#third is s, filtered on proc_type from eq_p

s = db.signal.select(columns=[kx.Column('name').name('sig_name'),kx.Column('id').name('s_id')],where=[kx.Column('proc_type_id').isin(eq_p['proc_type_id']),kx.Column('name').like('*Pressure*')])

#fourth is process_run_persisted on pr.equip_id = eq.id

pr = db.process_run_persisted.select(columns=[kx.Column('recipe'),kx.Column('start_time'),kx.Column('end_time'),kx.Column('equip_id')],where=[kx.Column('start_time') > '2024-11-22',kx.Column('start_time') < '2024-11-24',kx.Column('equip_id').isin('eq['tool_id']'),kx.Column('recipe').like('Process.QA*')])

sd = db.ees_sensor_data.select(columns=[kx.Column('sensor_id'),kx.Column('ts_id'),kx.Column('min_time'),kx.Column('max_time'),kx.Column('time_stamps'),kx.Column('data_float')],where=[kx.Column('date').within(pr['start_time'],min(), pr['end_time'].max()),kx.Column('min_time') >= pr['start_time'],min(),kx.Column('max_time') >= pr['end_time'].max(),kx.Column('sensor_id').isin(s['s_id'])])

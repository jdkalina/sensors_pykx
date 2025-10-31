import pykx as kx
from actipy import read_device
import os
from pathlib import Path

cwd = "clinical"
machine_ids = ['M2374','M2375']
data_dir = "data"
participants = os.listdir(Path.home() / cwd / data_dir)
print('v.0.1')

def process_data(machine_ids=machine_ids, data_dir=data_dir, cwd=cwd):
    for participant in participants:
        devices = os.listdir(Path.home() / cwd / data_dir / participant)
        for device in devices:
            if device in machine_ids:
                for file in os.listdir(Path.home() / cwd / data_dir / participant / device):
                    if file.endswith('.cwa.gz') or file.endswith('.cwa'): # add in additional filters to extract raw data. 
                        file_path = Path.home() / cwd / data_dir / participant / device / file
                        data, info = read_device(f"{file_path}", resample_hz=None)
                        data = data.reset_index()
                        print('data loaded from actipy')
                        types = {'time':kx.TimestampAtom,'x': kx.FloatAtom, 'y':kx.FloatAtom, 'z':kx.FloatAtom, 'temperature':kx.FloatAtom, 'light':kx.FloatAtom}
                        print(f"Step 1 {kx.q('.z.p')}")
                        data = kx.toq(data, ktype=types)
                        print(f"Step 2 {kx.q('.z.p')}")
                        data['date'] = data['time'].date
                        print(f"Step 3 {kx.q('.z.p')}")
                        data['participant'] = participant #symbolType
                        print(f"Step 4 {kx.q('.z.p')}")
                        data['updateTS'] = kx.q('.z.p')
                        print(f"Step 5 - Load into Q Memory {kx.q('.z.p')}")
                        kx.q[device] = data
                        print(f"Write into Sensors {kx.q('.z.p')}")
                        kx.q(f'.dbw.il.upsert[`{device};] each flip each `date xgroup {device}')
                        print(f"data upserted into sensors for {device} {kx.q('.z.p')}")

def kx_dt(text): 
    return kx.q(f'`date${text}')

def kx_time(text): 
    return kx.q(f'`datetime${text}')

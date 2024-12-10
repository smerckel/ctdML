from collections import namedtuple
import asyncio

import numpy as np
import glider_profiles.profiles as gp_profiles
import fast_gsw
import matplotlib.pyplot as plt
from matplotlib.backend_bases import MouseButton

import sqlite3
import json






class Database():
    def __init__(self, filename, experimentID):
        self.db_file = filename
        self.experimentID = experimentID
        self.initialize_database()
        
    def initialize_database(self):
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        cmd = f'CREATE TABLE IF NOT EXISTS selection_data (plotnumber INTEGER, experimentID TEXT, points TEXT, PRIMARY KEY (plotnumber, experimentID))'
        cursor.execute(cmd)
        cmd =  '''
        CREATE TABLE IF NOT EXISTS profile_data (
        plotnumber INTEGER,
        experimentID TEXT,
        C FLOAT,
        T FLOAT,
        pressure FLOAT,
        S FLOAT,
        profile_cast TEXT,
        FOREIGN KEY (plotnumber, experimentID) REFERENCES selection_data(plotnumber, experimentID)
        )
        '''
        cursor.execute(cmd)
        conn.commit()
        conn.close()

    def save_plot_data(self, plotnumber, pts_down, pts_up, profile_data_down, profile_data_up):
        """Save plot data to the database."""
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        cmd = f'INSERT OR REPLACE INTO selection_data (plotnumber, experimentID, points) VALUES (?, ?, ?)'
        p_dict = dict(down=pts_down.tolist(), up=pts_up.tolist()) # arrays cannot be serialised it seems.
        cursor.execute(cmd, (plotnumber, self.experimentID, json.dumps(p_dict)))
        cmd = 'INSERT INTO profile_data (plotnumber, experimentID, S, C, T, pressure, profile_cast) VALUES (?, ?, ?, ?, ?, ?, ?)'
        values = [(plotnumber, self.experimentID, _S, _C, _T, _pressure, "down") for (_S, _C, _T, _pressure) in zip(profile_data_down.SARaw,
                                                                                                                    profile_data_down.C,
                                                                                                                    profile_data_down.T,
                                                                                                                    profile_data_down.pressure)]
        values += [(plotnumber, self.experimentID, _S, _C, _T, _pressure, "up") for (_S, _C, _T, _pressure) in zip(profile_data_up.SARaw,
                                                                                                                   profile_data_up.C,
                                                                                                                   profile_data_up.T,
                                                                                                                   profile_data_up.pressure)]
        cursor.executemany(cmd, values)
        conn.commit()
        conn.close()

    def get_available_plot_numbers(self):
        """Retrieve a list of all plot numbers in the table."""
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        cmd = f"SELECT plotnumber FROM selection_data WHERE experimentID = ?"
        cursor.execute(cmd, (self.experimentID,))
        # Fetch all results and extract the first item from each row
        plot_numbers = [row[0] for row in cursor.fetchall()]
        conn.close()
        return plot_numbers
     
    def load_plot_data(self, plotnumber):
        """Load plot data from the database."""
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        cmd = f'SELECT points FROM selection_data WHERE plotnumber = ? and experimentID = ?'
        cursor.execute(cmd, (plotnumber, self.experimentID))
        result = cursor.fetchone()
        conn.close()
        if result:
            p_dict = json.loads(result[0])
            return p_dict["down"], p_dict["up"]
        else:
            return None

    def load_profile_data(self, plotnumber, profile_cast=''):
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        if profile_cast:
            cmd = f'SELECT C, T, pressure, S FROM profile_data WHERE plotnumber = ? and experimentID = ? and profile_cast = ?'
            cursor.execute(cmd, (plotnumber, self.experimentID, profile_cast))
        else:
            cmd = f'SELECT C, T, pressure, S FROM profile_data WHERE plotnumber = ? and experimentID = ?'
            cursor.execute(cmd, (plotnumber, self.experimentID))
        result = cursor.fetchall()
        conn.close()
        C, T, pressure, SA = np.asarray(result).T
        R = namedtuple("CTDValues", "C T pressure SA".split())
        return R(C, T, pressure, SA)
    
    
class Labeller():
    """
    """
    def __init__(self, experimentID, data):
        self.experimentID = experimentID
        self.ps = gp_profiles.ProfileSplitter(data)
        self.ps.split_profiles()
        self.ip = InteractivePlot()
        self.db = Database("labelled_data.db", experimentID)

    def label_profiles(self, i_start=0, i_end=-1, stride=1):
        if i_end<0:
            i_end += self.ps.nop
        already_processed_profiles = self.db.get_available_plot_numbers()
        for i, (down,up) in enumerate(self.ps.get_down_up_casts()):
            pts = None
            if i%stride != 0:
                continue
            if i<i_start or i>i_end:
                continue
            if i in already_processed_profiles:
                print(f"Skipping profile {i}, because it is already processed.")
                continue
            self.ip.plot_profiles(down, up)
            pts_down = self.ip.mark_range(s="Select steady Salinity range on down cast")
            if not pts_down is None:
                pts_up = self.ip.mark_range(s="Select steady Salinity range on up cast")
            print("Continue")
            #pts = plt.ginput(1, show_clicks=False)
            #if not pts:
            #    break
            #else:
            #    self.db.save_plot_data(i,pts_down[:,1], pts_up[:,1], down, up)
        if not pts:
            print("Ended prematurely")
            



class InteractivePlot():
    """
    """
    def __init__(self):
        self.f, self.ax = plt.subplots(1,2)
        self.f.canvas.mpl_connect('key_press_event', self.on_keypress)
        self.f.canvas.mpl_connect('button_press_event', self.on_click)
        self.queue = asyncio.queues.Queue()
        self.record=[]
        
    def on_click(self, event):
        pressure = None
        if event.button is MouseButton.LEFT and event.inaxes:
            pressure = event.ydata
            event.inaxes.plot(event.xdata, event.ydata,'C3+')
            self.record.append(pressure)
        if len(self.record)==2:
            self.queue.put_nowait(dict(pressure_range=self.record.copy()))
            self.record.clear()
            
            
    def on_keypress(self, event):
        match event.key:
            case 'c' | 'C':
                action = "continue"
            case 'q' | 'Q':
                action = "quit"
            case _:
                action = None
        print(action)
        
    def plot_profiles(self, downcast, upcast):
        for _ax in self.ax:
            _ax.cla()
        self.ax[0].plot(downcast.SARaw, -downcast.pressure, color='C0', label='downcast')
        self.ax[0].plot(upcast.SARaw, -upcast.pressure, color='C3', label='upcast')
        self.ax[1].plot(downcast.T, -downcast.pressure, color='C0', label='downcast')
        self.ax[1].plot(upcast.T, -upcast.pressure, color='C3', label='upcast')
        self.ax[0].legend(loc='upper right')
        self.ax[1].legend(loc='upper left')
        self.ax[0].set_title('Salinity')
        self.ax[1].set_title('Temperature')

    async def plot_data(self):
        x = np.arange(10)
        y = x**2
        self.ax[0].plot(x,y)
        self.ax[1].plot(x,y)
        plt.draw()
        await asyncio.sleep(2)
        
    async def mark_range(self, s):
        print(s)
        result = await self.queue.get()
        print(result)
        return result
        # while True:
        #     pts = np.asarray(plt.ginput(n=2, show_clicks=True))
        #     number_of_points, *_ = pts.shape
        #     if number_of_points != 1:
        #         break
        # if number_of_points == 2:
        #     for _ax in self.ax:
        #         _ax.hlines(pts[:,1], *_ax.get_xlim(), ls='--')
        # else:
        #     pts = None
        # return pts

    async def matplotlib_events(self):
        while True:
            plt.pause(0.05)
            await asyncio.sleep(0.05)
    async def process(self):
        while True:
            await self.mark_range("autopilot")
        
    async def run(self):
        task0 = asyncio.create_task(self.matplotlib_events())
        task1 = asyncio.create_task(self.plot_data())
        task2 = asyncio.create_task(self.process())
        await asyncio.gather(task0, task1, task2)
        
            
            
            

ip = InteractivePlot()
asyncio.run(ip.run())

#loop = asyncio.new_event_loop()
#loop.run_until_complete(ip.run())
#print("done")
# #db = Database("labelled_data.db", "HL2014Sebastian")
# #Q

# import dbdreader
# dbd = dbdreader.MultiDBD("/home/lucas/gliderdata/helgoland201407/hd/sebastian-*[de]bd", max_files=100)
# dbd.dbds['eng'] = dbd.dbds['eng']
# dbd.dbds['sci'] = dbd.dbds['sci']
# tctd, C, T, D = dbd.get_CTD_sync()
# SA = fast_gsw.SA(C*10, T, D*10, 54, 8)
# data = dict(time=tctd, C=C*10, T=T, pressure=D, D=D*10, SARaw = SA)


# labeller = Labeller("HL2014Sebastian", data)


# labeller.label_profiles(stride=100)

# plt.show()

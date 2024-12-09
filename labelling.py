import numpy as np
import glider_profiles.profiles as gp_profiles
import fast_gsw
import matplotlib.pyplot as plt
import sqlite3
import json


import dbdreader
dbd = dbdreader.MultiDBD("/home/lucas/gliderdata/helgoland201407/hd/sebastian-*[de]bd")
dbd.dbds['eng'] = dbd.dbds['eng'][100:150]
dbd.dbds['sci'] = dbd.dbds['sci'][100:150]
tctd, C, T, D = dbd.get_CTD_sync()
SA = fast_gsw.SA(C*10, T, D*10, 54, 8)
data = dict(time=tctd, C=C*10, T=T, pressure=D, D=D*10, SARaw = SA)


class InteractivePlot():
    """
    """
    def __init__(self):
        self.f, self.ax = plt.subplots(1,1)
        
    def plot_profiles(self, downcast, upcast):
        self.ax.cla()
        self.ax.plot(downcast.SARaw, -downcast.pressure, color='C0')
        self.ax.plot(upcast.SARaw, -upcast.pressure, color='C3')
        
    def mark_range(self, s):
        print(s)
        while True:
            pts = np.asarray(plt.ginput(n=2, show_clicks=True))
            number_of_points, *_ = pts.shape
            if number_of_points != 1:
                break
        if number_of_points == 2:
            self.ax.hlines(pts[:,1], *self.ax.get_xlim(), ls='--')
        else:
            pts = None
        return pts






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
        pressure FLOAT,
        S FLOAT,
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
        cmd = 'INSERT INTO profile_data (plotnumber, experimentID, S, pressure) VALUES (?, ?, ?, ?)'
        values = [(plotnumber, self.experimentID, _S, _pressure) for (_S, _pressure) in zip(profile_data_down.SARaw,
                                                                                            profile_data_down.pressure)]
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

    def load_profile_data(self, plotnumber):
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        cmd = f'SELECT pressure, S FROM profile_data WHERE plotnumber = ? and experimentID = ?'
        cursor.execute(cmd, (plotnumber, self.experimentID))
        result = cursor.fetchall()
        conn.close()
        pressure, SA = np.asarray(result).T
        return pressure, SA
    
    
class Labeller():
    """
    """
    def __init__(self, experimentID, data):
        self.experimentID = experimentID
        self.ps = gp_profiles.ProfileSplitter(data)
        self.ps.split_profiles()
        self.ip = InteractivePlot()
        self.db = Database("labelled_data.db", experimentID)

    def label_profiles(self, i_start=0, i_end=-1):
        if i_end<0:
            i_end += self.ps.nop
        for i, (down,up) in enumerate(self.ps.get_down_up_casts()):
            if i<i_start or i>i_end:
                continue
            self.ip.plot_profiles(down, up)
            pts_down = self.ip.mark_range(s="Select steady Salinity range on down cast")
            if not pts_down is None:
                pts_up = self.ip.mark_range(s="Select steady Salinity range on up cast")
            print("Continue")
            pts = plt.ginput(1, show_clicks=False)
            if not pts:
                break
            else:
                self.db.save_plot_data(i,pts_down[:,1], pts_up[:,1], down, up)
        if not pts:
            print("Ended prematurely")
            
            
#db = Database("labelled_data.db", "HL2014Sebastian")
#Q

labeller = Labeller("HL2014Sebastian", data)

labeller.label_profiles(i_end=3)

plt.show()

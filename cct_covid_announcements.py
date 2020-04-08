import pandas as pd
import numpy as np
import math
from bokeh.core.properties import Instance
from bokeh.models import ColumnDataSource, TapTool, OpenURL
from bokeh.palettes import Dark2
from bokeh.plotting import figure
from bokeh.transform import factor_cmap
from bokeh.models import NumeralTickFormatter,  DatetimeTickFormatter, DaysTicker, FuncTickFormatter,  HoverTool, formatters, FactorRange, FixedTicker, DaysTicker


df_event = pd.read_csv('data/public/covid_key_announcements.csv')
df_event['Date'] =  pd.to_datetime(df_event['Date'], format='%d-%m-%Y')
df_event_sort = df_event.sort_values(["Date", "Entity"], ascending = (True, True))
df_event_sort['seq']=df_event_sort.groupby(['Date']).cumcount()
df_event_sort['Date'] =  pd.to_datetime(df_event_sort['Date'], format='%d-%m-%Y')

new_customdatadf  = np.stack((df_event_sort['Decision_Title'], df_event_sort['Decision_Description'], df_event_sort['Link']), axis=-1)

source = ColumnDataSource(df_event_sort)

entity = df_event_sort['Entity'].unique()

earlier = df_event_sort['Date'].max() - df_event_sort['Date'].min()


p = figure(
           y_range=(0, 5+df_event_sort['seq'].max()),
           plot_height=250, 
           title=None,
           toolbar_location=None, 
           x_axis_type='datetime',
           tools="tap",
           #plot_width=800,
           sizing_mode='stretch_width',
           )

p.square(x='Date', 
         y='seq', 
         source=source, 
         legend_field='Entity',
         fill_color=factor_cmap('Entity', palette=Dark2[6], factors=entity),
         #fill_alpha=0.6,
         line_color=None,
         size=8)

p.legend.label_text_font_size='6pt'

p.xaxis.axis_label_text_font_size='5pt'
p.xaxis.formatter =  DatetimeTickFormatter(days=["%b-%d"])
p.xgrid.grid_line_color = None
p.xaxis.major_label_orientation = math.pi/4
p.xaxis.ticker = DaysTicker(days=list(range(1, len(df_event_sort['Date']))))
p.xaxis.axis_label_text_font_size = "7pt"

p.y_range.start = -1
p.yaxis.visible = False

p.legend.orientation = "horizontal"
p.legend.location = 'top_center' #(0,190)
p.legend.glyph_width = 12


hover = HoverTool(tooltips =
                          """  
                            <div>
                                <span style="font-size: 9px; 
                                            font-weight: bold; 
                                            font-family:sans-serif">
                                    <time datetime=@Date>
                                       @Date   
                                    </time>
                                    
                                    
                                </span>
                            </div>
                            
                            <div>
                                <span style="font-size: 9px; 
                                            font-family:sans-serif">
                                            
                                    @Entity
                                    
                                </span>
                            </div>  
                            <div>
                                <span style="font-size: 12px;
                                            font-weight: bold;
                                            color:black;
                                            font-family:sans-serif">
                                            
                                    @Decision_Title
                                    
                                </span>
                            </div> 
                            <div>
                                <span style="font-size: 11px; 
                                            font-family:sans-serif">
                                            
                                    @Decision_Description
                                    
                                </span>
                            </div>
                            <div>
                                <span style="font-size: 9px;color:red; font-family:sans-serif">
                                
                                    click on square to open link to article
                                    
                                </span>
                             </div>
                            """
                 )



# Add the hover tool to the graph
p.add_tools(hover)

url = "@Link"
taptool = p.select(type=TapTool)
taptool.callback = OpenURL(url=url)

show(p)







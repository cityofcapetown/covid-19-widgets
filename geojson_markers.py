from folium.features import GeoJson
from folium.map import FeatureGroup
from jinja2 import Template


class GeoJsonMarkers(FeatureGroup):
    """
    Populates a layer of markers from GeoJson point data.

    Roughly follows the API of the fast_marker_cluster class,
    but borrows a lot from the GeoJson class.

    Experimental, so assumes that the user is doing the right thing (tm)
    in terms of passing in sensible GeoJSON point data.
    """

    _template = Template("""
        {% macro script(this, kwargs) %}
            var {{ this.get_name() }} = (function(){
                {%- if this.tooltip %}
                {{ this.tooltip_callback }}
                {% endif -%}

                {{ this.callback }}

                var feature_group = L.featureGroup({{ this.options|tojson }});

                {% if not this.embed %}
                $.ajax({url: {{ this.embed_link|tojson }}, dataType: 'json', async: true,
                    success: function(fetched_data) {
                {% endif %}

                    var data = {{ this.data|tojson if this.embed else "fetched_data" }};        
                    for (var i = 0; i < data.features.length; i++) {
                        var feature = data.features[i];
                        var marker = callback(feature);
                        {%- if this.tooltip %}
                        marker.bindTooltip(tooltip_callback(feature));
                        {% endif -%}

                        marker.addTo(feature_group);
                    }

                {% if not this.embed %}
                }});
                {% endif %}

                return feature_group;                
            })();
            {{ this.get_name() }}.addTo({{ this._parent.get_name() }});
        {% endmacro %}
    """)

    def __init__(self, data, embed=True, callback=None, tooltip=True, tooltip_callback=None,
                 name=None, overlay=True, control=True, show=True):
        super(GeoJsonMarkers, self).__init__(name=name, overlay=overlay, control=control, show=show)

        self._name = 'GeoJsonMarkers'
        self.embed = embed
        self.embed_link = None
        self.tooltip = tooltip

        self.data = GeoJson.process_data(self, data)

        if callback is None:
            self.callback = """
                var callback = function (feature) {
                    var icon = L.AwesomeMarkers.icon();
                    var coords = feature.geometry.coordinates;
                    var marker = L.marker(new L.LatLng(coords[1], coords[0]));
                    marker.setIcon(icon);

                    return marker;
                };"""
        else:
            self.callback = 'var callback = {};'.format(callback)

        if tooltip_callback is None:
            self.tooltip_callback = """
                var tooltip_callback = function (feature) {
                    let handleObject = (feature)=>typeof(feature)=='object' ? JSON.stringify(feature) : feature;
                    return '<table>' +
                        String(
                            Object.keys(feature.properties).map(
                                columnname=>
                                    `<tr style="text-align: left;">
                                    <th style="padding: 4px; padding-right: 10px;">
                                        ${ handleObject(columnname).toLocaleString() }
                                    </th>
                                    <td style="padding: 4px;">
                                        ${ handleObject(feature.properties[columnname]).toLocaleString() }
                                    </td></tr>`
                            ).join('')
                        )
                        + '</table>'
                };
            """
        else:
            self.tooltip_callback = 'var tooltip_callback = {};'.format(tooltip_callback)

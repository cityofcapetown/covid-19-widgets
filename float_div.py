import folium
import jinja2


class FloatDiv(folium.map.Layer):
    """Adds a floating div in HTML canvas on top of the map."""
    _template = jinja2.Template("""
            {% macro header(this,kwargs) %}
                <style>
                    #{{this.get_name()}} {
                        position:absolute;
                        top:{{this.top}}%;
                        left:{{this.left}}%;
                        }
                </style>
            {% endmacro %}

            {% macro html(this,kwargs) %}
            <div id="{{this.get_name()}}" alt="float_div" style="z-index: 999999"> </div>
            {% endmacro %}

            {% macro script(this, kwargs) %}

            function {{ this.get_name() }}_add () {
                document.getElementById("{{ this.get_name() }}").innerHTML = "{{ this.content }}";
            }
            function {{ this.get_name() }}_remove () {
                document.getElementById("{{ this.get_name() }}").innerHTML = "";
            }

            var {{ this._parent.get_name() }}_parent_add_func = {{ this._parent.get_name() }}.onAdd;
            {{ this._parent.get_name() }}.onAdd = function(map){
                {{ this.get_name() }}_add();
                {{ this._parent.get_name() }}_parent_add_func.call(this, map);
            };

            var {{ this._parent.get_name() }}_parent_remove_func = {{ this._parent.get_name() }}.onRemove;
            {{ this._parent.get_name() }}.onRemove = function(map){
                {{ this.get_name() }}_remove ();
                {{ this._parent.get_name() }}_parent_remove_func.call(this, map);
            };

            {% if this.show %}
                {{ this.get_name() }}_add();
            {% endif %}

            {% endmacro %}
            """)

    def __init__(self, content, top=10, left=0):
        super(FloatDiv, self).__init__()
        self._name = 'FloatDiv'
        self.content = content
        self.top = top
        self.left = left
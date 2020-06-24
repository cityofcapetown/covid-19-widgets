from collections.abc import Sequence, Mapping
import json

from branca.element import CssLink, JavascriptLink
import folium
import folium.utilities
from jinja2 import Template


class TreeLayerControl(folium.MacroElement):
    """
    Creates a LayerControl object to be added on a folium map.

    This object should be added to a Map object. Only Layer children
    of Map are included in the layer control.

    Parameters
    ----------
    base_tree_entries: Sequence or Mapping
          The base layer tree entries. This is wrapped in the initial base tree.
    overlay_tree_entries: Sequence or Mapping
          The overlay layer tree entries. This is wrapped in the initial overlay tree.
    base_tree_entries_properties: Mapping, default {base_tree_label: {"noShow": True}}
          Flat dictionary for passing in properties for specific tree entries. See
          https://github.com/jjimenezshaw/Leaflet.Control.Layers.Tree#tricks-about-the-tree
    overlay_tree_entries_properties: Mapping, default {overlay_tree_label: {"noShow": True}}
          Flat dictionary for passing in properties for specific tree entries. See
          https://github.com/jjimenezshaw/Leaflet.Control.Layers.Tree#tricks-about-the-tree
    base_tree_label: str, default: 'Base Layers'
          Label to use for the base layer tree.
    overlay_tree_label: str, default: 'Overlays'
          Label to use for the overlay tree label.
    position : str
          The position of the control (one of the map corners), can be
          'topleft', 'topright', 'bottomleft' or 'bottomright'
          default: 'topright'
    collapsed : bool, default True
          If true the control will be collapsed into an icon and expanded on
          mouse hover or touch.
    autoZIndex : bool, default True
          If true the control assigns zIndexes in increasing order to all of
          its layers so that the order is preserved when switching them on/off.
    **kwargs
        Additional (possibly inherited) options. See
        https://github.com/jjimenezshaw/Leaflet.Control.Layers.Tree#api

    """
    _js_key = 'L.Control.Layers.Tree.js'
    _js_link = 'https://cdn.jsdelivr.net/npm/leaflet.control.layers.tree@1.0.0/L.Control.Layers.Tree.js'
    _css_key = 'L.Control.Layers.Tree.css'
    _css_link = 'https://cdn.jsdelivr.net/npm/leaflet.control.layers.tree@1.0.0/L.Control.Layers.Tree.css'

    _template = Template("""
        {% macro script(this,kwargs) %}
            var baseTree_{{ this.get_name() }} = {{ this.base_tree }};
            var overlayTree_{{ this.get_name() }} = {{ this.overlay_tree }};

            L.control.layers.tree(
                baseTree_{{ this.get_name() }}, 
                overlayTree_{{ this.get_name() }}, 
                {{ this.options|tojson }}
            ).addTo({{ this.parent_map.get_name() }});

            {%- for val in this.layers_untoggle %}
            {{ val }}.remove();
            {%- endfor %}
        {% endmacro %}
        """)

    def __init__(self, base_tree_entries=None, overlay_tree_entries=None,
                 base_tree_entries_properties=None, overlay_tree_entries_properties=None,
                 base_tree_label="Base Layers", overlay_tree_label="Overlays",
                 position='topright', collapsed=True, autoZIndex=True,
                 **kwargs):
        super().__init__()
        self._name = 'TreeLayerControl'
        self.options = folium.utilities.parse_options(
            position=position,
            collapsed=collapsed,
            autoZIndex=autoZIndex,
            **kwargs
        )

        self.base_tree_entries = {
            base_tree_label: base_tree_entries
        } if base_tree_entries else None
        self.base_tree_entries_properties = {base_tree_label: {"noShow": True}}
        self.base_tree_entries_properties = {
            **self.base_tree_entries_properties,
            **(base_tree_entries_properties if base_tree_entries_properties else {})
        }

        self.overlay_tree_entries = {
            overlay_tree_label: overlay_tree_entries
        } if overlay_tree_entries else None
        self.overlay_tree_entries_properties = {overlay_tree_label: {"noShow": True}}
        self.overlay_tree_entries_properties = {
            **self.overlay_tree_entries_properties,
            **(overlay_tree_entries_properties if overlay_tree_entries_properties else {})
        }

        self.base_tree = ""
        self.overlay_tree = ""
        self.layers_untoggle = []
        self.parent_map = None
        self.layers = []

    def _recursive_tree_convert(self, tree_dict, tree_layer_properties):
        def _handle_child_value(child):
            if isinstance(child, folium.map.Layer):
                child_dict = {"label": child.layer_name, "layer": child.get_name()}
            elif isinstance(child, Mapping):
                child_dict = list(self._recursive_tree_convert(child, tree_layer_properties))[0]
            else:
                child_dict = {"label": child}

            return {
                **child_dict,
                **tree_layer_properties.get(child_dict["label"], {})
            }

        for key, values in tree_dict.items():
            children = [
                    _handle_child_value(child)
                    for child in values
            ]
            self.layers += [child["layer"] for child in children if "layer" in child]

            tree_entry = {
                "label": key,
                "children": children,
                **tree_layer_properties.get(key, {})
            }

            yield tree_entry

    def _make_layers_dirty(self, tree_string):
        new_tree_string = tree_string
        for layer in self.layers:
            # this is undoing the JSON string sanitising
            new_tree_string = new_tree_string.replace(f'"{layer}"', layer)

        return new_tree_string

    def render(self, **kwargs):
        """Renders the HTML representation of the element."""
        self.layers_untoggle = [
            item.get_name()
            for item in self._parent._children.values()
            if isinstance(item, folium.map.Layer) and item.control and not item.show
        ]

        self.parent_map = folium.utilities.get_obj_in_upper_tree(self, folium.Map)

        self.base_tree = json.dumps(list(
            self._recursive_tree_convert(
                self.base_tree_entries,
                self.base_tree_entries_properties
            )
        )[0]) if self.base_tree_entries else None
        self.base_tree = self._make_layers_dirty(self.base_tree) if self.base_tree else "null"

        self.overlay_tree = json.dumps(list(
            self._recursive_tree_convert(
                self.overlay_tree_entries,
                self.overlay_tree_entries_properties
            )
        )[0]) if self.overlay_tree_entries else None
        self.overlay_tree = self._make_layers_dirty(self.overlay_tree) if self.overlay_tree else "null"

        root = self.get_root()
        root.header.add_child(
            JavascriptLink(self._js_link),
            name=self._js_key
        )

        root.header.add_child(
            CssLink(self._css_link),
            name=self._css_key
        )

        super().render()

from honeyflare import _coerce_attribute_value


def test_primitives_pass_through():
    assert _coerce_attribute_value("foo") == "foo"
    assert _coerce_attribute_value(42) == 42
    assert _coerce_attribute_value(3.14) == 3.14
    assert _coerce_attribute_value(True) is True
    assert _coerce_attribute_value(False) is False


def test_list_of_primitives_passes_through():
    assert _coerce_attribute_value(["a", "b", "c"]) == ["a", "b", "c"]
    assert _coerce_attribute_value([1, 2, 3]) == [1, 2, 3]
    assert _coerce_attribute_value((1, 2, 3)) == [1, 2, 3]


def test_list_filters_nones():
    """OTel rejects lists containing None; drop them."""
    assert _coerce_attribute_value(["a", None, "b"]) == ["a", "b"]


def test_dict_becomes_json_string():
    """Cloudflare ships ResponseHeaders, Cookies etc. as dicts —
    libhoney JSON-serialized them; match that behavior so they land in
    Honeycomb instead of being dropped with a warning."""
    result = _coerce_attribute_value({"content-type": "text/html", "x-foo": "bar"})
    assert isinstance(result, str)
    # orjson sorts keys deterministically — not guaranteed but useful
    # for the assertion. If behavior shifts, compare parsed form.
    assert "content-type" in result
    assert "text/html" in result


def test_mixed_type_list_becomes_json_string():
    """A list with non-primitive elements serializes to JSON as a whole."""
    result = _coerce_attribute_value([{"foo": "bar"}, {"baz": "qux"}])
    assert isinstance(result, str)
    assert "foo" in result
    assert "bar" in result


def test_nested_structure_serializes_fully():
    result = _coerce_attribute_value({"outer": {"inner": [1, 2, 3]}})
    assert isinstance(result, str)
    assert "inner" in result

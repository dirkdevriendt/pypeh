import pytest

from peh_model.peh import ObservableProperty

from pypeh import NamespaceManager


@pytest.fixture
def nm():
    return NamespaceManager()


@pytest.mark.core
class TestNamespaces:
    def test_bind_namespace(self, nm):
        nm.bind("peh", "https://w3id.org/peh")
        assert "peh" in nm.namespaces
        assert nm.namespaces["peh"] == "https://w3id.org/peh/"

    def test_register_class(self, nm):
        nm.register_class(ObservableProperty, "obsprop")
        assert nm.class_prefixes[ObservableProperty] == "obsprop"

    def test_register_non_dataclass_raises(self, nm):
        class NotADataClass:
            pass

        with pytest.raises(TypeError):
            nm.register_class(NotADataClass, "project")

    def test_mint_without_namespace_raises(self, nm):
        nm.register_class(ObservableProperty, "project")
        p = ObservableProperty("question")
        with pytest.raises(ValueError):
            nm.mint(p)

    def test_mint_without_class_registration_raises(self, nm):
        nm.bind("project", "https://w3id.org/peh")
        p = ObservableProperty("question")
        with pytest.raises(ValueError):
            nm.mint(p)

    def test_mint_generates_valid_iri(self, nm):
        nm.bind("project", "https://w3id.org/peh")
        nm.register_class(ObservableProperty, "project")
        p = ObservableProperty("question")
        iri = nm.mint(p)
        assert iri.startswith("https://w3id.org/peh/project/")
        assert len(iri.split("/")[-1]) > 0  # suffix exists

    def test_custom_suffix_strategy(self, nm):
        nm.bind("project", "https://w3id.org/peh")
        nm.register_class(ObservableProperty, "project")
        nm.set_suffix_strategy(lambda obj: "fixed-id")
        p = ObservableProperty("question")
        iri = nm.mint(p)
        assert iri.endswith("/fixed-id")

    def test_hash_suffix_strategy(self, nm):
        nm.bind("project", "https://w3id.org/peh")
        nm.register_class(ObservableProperty, "project")
        nm.suffix_strategy = nm.default_suffix(length=8)
        p = ObservableProperty("question")
        iri = nm.mint(p)
        suffix = iri.split("/")[-1]
        assert len(suffix) == 8
        # assert hashing is deterministic
        iri2 = nm.mint(p)
        assert iri == iri2

    def test_mint_and_set(self, nm):
        nm.bind("project", "https://w3id.org/peh")
        nm.register_class(ObservableProperty, "project")
        nm.suffix_strategy = nm.default_suffix(length=8)
        p = ObservableProperty(id="temp", ui_label="question")
        iri = nm.mint(p)
        nm.mint_and_set(p)
        assert p.id == iri

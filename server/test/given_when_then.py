class GWT:

    def __init__(self, test_case, args, kwargs):
        self.test_case = test_case
        self.args = args
        self.kwargs = kwargs

    def given(self):
        raise NotImplementedError("No given function defined!")

    def when(self):
        raise NotImplementedError("No when function defined!")

    def then(self):
        raise NotImplementedError("No then function defined!")


def given_when_then_test(test_class):
    """ Mark a class for running as an given-when-then test.

    test_class must be a GTW implementation.

    :type test_class: type(GWT)
    :return: function(self, *args, **kwargs)
    """
    def new_test(self, *args, **kwargs):
        test_object = test_class(self, args, kwargs)
        test_object.given()
        test_object.when()
        test_object.then()
    new_test.__name__ = test_class.__name__
    return new_test

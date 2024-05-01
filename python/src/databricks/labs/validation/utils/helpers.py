class Helpers:

    @staticmethod
    def to_java_array(py_array, sc):
        if isinstance(py_array[0], str):
            java_string_class = sc._jvm.java.lang.String
            java_array = sc._gateway.new_array(java_string_class, len(py_array))
            for i in range(len(py_array)):
                java_array[i] = py_array[i]
        else:
            raise Exception("Only List of Strings is currently supported.")
        return java_array

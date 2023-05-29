(cl:in-package #:vellum-duckdb)


(defmethod vellum:copy-from ((format (eql ':duckdb))
                             input
                             &rest options
                             &key columns (header (apply #'vellum.header:make-header columns))
                               (parameters nil parameters-bound-p))
  (declare (ignore options parameters-bound-p))
  (let ((columns (ddb:query input parameters))
        (result (vellum:make-table :header header)))
    (iterate
      (declare (ignorable column-name))
      (for (column-name . column-content) in columns)
      (for i from 0)
      (vellum:transform result
                        (let ((index 0))
                          (vellum:bind-row ()
                            (setf (vellum:rr i) (aref column-content index))
                            (incf index)))
                        :end (length column-content)
                        :enable-restarts nil
                        :wrap-errors nil
                        :in-place t))
    result))

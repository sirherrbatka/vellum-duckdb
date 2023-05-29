(cl:in-package #:vellum-duckdb)


(defun copy-into-data-frame (result data-frame)
  (bind ((p-result (duckdb::handle result))
         (chunk-count (duckdb-api:result-chunk-count p-result))
         (column-count (duckdb-api:duckdb-column-count p-result))
         (result-alist
          (iterate
            (for column-index below column-count)
            (collecting (cons (duckdb-api:duckdb-column-name p-result column-index)
                              (make-array 1024 :adjustable t :fill-pointer 0)))))
         (j 0)
         (transformation (vellum.table:transformation data-frame
                                                      (vellum:bind-row ()
                                                        (iterate
                                                          (declare (ignorable column-name))
                                                          (for i from 0 below column-count)
                                                          (for (column-name . column-chunk) in result-alist)
                                                          (setf (vellum:rr i) (aref column-chunk j))))
                                                      :in-place t
                                                      :start 0))
         ((:flet copy-in-chunk (chunk))
          (iterate
            (repeat (nth-value 1 (duckdb::translate-chunk result-alist chunk)))
            (vellum.table:transform-row transformation)
            (incf j))))
    (iterate
      (for chunk-index below chunk-count)
      (duckdb-api:with-data-chunk (chunk p-result chunk-index)
        (copy-in-chunk chunk)))
    (vellum.table:transformation-result transformation)))


(defmethod vellum:copy-from ((format (eql ':duckdb))
                             input
                             &rest options
                             &key columns (header (apply #'vellum.header:make-header columns))
                               (parameters nil parameters-bound-p))
  (declare (ignore options))
  (let ((data-frame (vellum:make-table :header header)))
    (duckdb:with-statement (statement input :connection duckdb:*connection*)
      (when parameters-bound-p
        (duckdb:bind-parameters statement parameters))
      (duckdb:with-execute (result statement)
        (copy-into-data-frame result data-frame)))))

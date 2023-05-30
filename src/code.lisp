(cl:in-package #:vellum-duckdb)


(defun copy-into-data-frame (result header)
  (bind ((p-result (duckdb::handle result))
         (chunk-count (duckdb-api:result-chunk-count p-result))
         (column-count (duckdb-api:duckdb-column-count p-result))
         (result-alist
          (iterate
            (for column-index below column-count)
            (collecting (cons (duckdb-api:duckdb-column-name p-result column-index)
                              (make-array 1024 :adjustable t :fill-pointer 0)))))
         (data-frame (if header
                         (vellum:make-table :header header)
                         (vellum:make-table :columns (mapcar #'car result-alist))))
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
         ((:flet copy-chunk (chunk))
          (iterate
            (repeat (nth-value 1 (duckdb::translate-chunk result-alist chunk)))
            (vellum.table:transform-row transformation)
            (incf j))))
    (iterate
      (for chunk-index below chunk-count)
      (duckdb-api:with-data-chunk (chunk p-result chunk-index)
        (copy-chunk chunk)))
    (vellum.table:transformation-result transformation)))


(defmethod vellum:copy-from ((format (eql ':duckdb))
                             input
                             &rest options
                             &key (columns nil columns-bound-p)
                               (header (if columns-bound-p (apply #'vellum.header:make-header columns) nil))
                               (parameters nil parameters-bound-p))
  (declare (ignore options))
  (duckdb:with-statement (statement input :connection duckdb:*connection*)
    (when parameters-bound-p
      (duckdb:bind-parameters statement parameters))
    (duckdb:with-execute (result statement)
      (copy-into-data-frame result header))))

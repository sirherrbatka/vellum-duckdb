(cl:in-package #:vellum-duckdb)


(defun lisp-type<-duckdb-type (type)
  (or (switch (type)
        (:duckdb-bigint '(signed-byte 64))
        (:duckdb-ubigint '(unsigned-byte 64))
        (:duckdb-integer '(signed-byte 32))
        (:duckdb-uinteger '(unsigned-byte 32))
        (:duckdb-float 'single-float)
        (:duckdb-double 'double-float)
        (:duckdb-varchar 'string)
        (:duckdb-timestamp 'local-time:timestamp)
        (:duckdb-tinyint '(signed-byte 8))
        (:duckdb-utinyint '(unsigned-byte 8))
        (:duckdb-smallint '(signed-byte 16))
        (:duckdb-usmallint '(unsigned-byte 16))
        (:duckdb-boolean 'boolean)
        (:duckdb-date 'local-time:date)
        (:duckdb-blob 'vector)
        (:duckdb-hugeint 'integer))
      t))


(defun copy-into-data-frame (result header)
  (bind ((p-result (duckdb::handle result))
         (chunk-count (duckdb-api:result-chunk-count p-result))
         (column-count (duckdb-api:duckdb-column-count p-result))
         ((:values result-alist types)
          (iterate
            (for column-index below column-count)
            (collecting (duckdb-api:duckdb-column-type p-result column-index)
              into types)
            (collecting (cons (duckdb-api:duckdb-column-name p-result column-index)
                              (make-array 1024 :adjustable t :fill-pointer 0))
              into result)
            (finally (return (values result types)))))
         (data-frame (if header
                         (vellum:make-table :header header)
                         (vellum:make-table :columns (mapcar (lambda (name.buffer type &aux (name (car name.buffer)))
                                                               `(:name ,name :type ,(lisp-type<-duckdb-type type)))
                                                             result-alist
                                                             types))))
         (j 0)
         (transformation (vellum.table:transformation data-frame
                                                      (vellum:bind-row ()
                                                        (iterate
                                                          (declare (ignorable column-name))
                                                          (for i from 0 below column-count)
                                                          (for (column-name . column-chunk) in result-alist)
                                                          (setf (vellum:rr i) (aref column-chunk j))))
                                                      :enable-restarts nil
                                                      :wrap-errors nil
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
                             &key
                               (columns nil columns-bound-p)
                               (header (if columns-bound-p (apply #'vellum.header:make-header columns) nil))
                               (parameters nil parameters-bound-p)
                             &aux
                               (duckdb:*sql-null-return-value* :null))
  (declare (ignore options))
  (duckdb:with-statement (statement input :connection duckdb:*connection*)
    (when parameters-bound-p
      (duckdb:bind-parameters statement parameters))
    (duckdb:with-execute (result statement)
      (copy-into-data-frame result header))))

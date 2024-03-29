;; # Chicago bike trips - time analysis

(ns index
  (:require [tablecloth.api :as tc]
            [tech.v3.datatype.datetime :as datetime]
            [scicloj.noj.v1.vis.hanami :as hanami]
            [aerial.hanami.templates :as ht]
            [scicloj.kindly.v4.kind :as kind]))

(defonce raw-trips
  (-> "data/202304_divvy_tripdata.csv.gz"
      (tc/dataset {:parser-fn {"started_at"
                               [:local-date-time "yyyy-MM-dd HH:mm:ss"]}
                   :key-fn keyword})))


(def processed-trips
  (-> raw-trips
      (tc/add-column
       :hour
       (fn [ds]
         (-> ds
             :started_at
             (->> (datetime/long-temporal-field :hours)))))
      (tc/add-column
       :day-of-week
       (fn [ds]
         (-> ds
             :started_at
             (->> (datetime/long-temporal-field :day-of-week)))))))


(defn plot-hours [trips]
  (-> trips 
      (tc/group-by [:hour])
      (tc/aggregate {:n tc/row-count})
      (hanami/plot ht/bar-chart
                   {:X "hour"
                    :Y "n"})))


(-> processed-trips
    plot-hours)

(-> processed-trips
    plot-hours
    kind/portal)

(-> processed-trips
    (tc/group-by [:day-of-week :hour])
    (tc/aggregate {:n tc/row-count})
    (tc/group-by [:day-of-week])
    (hanami/plot ht/bar-chart
                 {:X "hour"
                  :Y "n"})
    (tc/order-by [:day-of-week]))

package main

import (
	"database/sql"
	"log"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"sort"
)

type Pair struct {
	Key string
	Value float32
}
type PairList []Pair

func (p PairList) Len() int           { return len(p) }
func (p PairList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p PairList) Less(i, j int) bool { return p[i].Value > p[j].Value }

func main() {

	stockMap := make(map[string]float32)

	//////////////////////////// mysql db connect ////////////////////////////
	db, err := sql.Open("mysql", "erpy:kiwitomato.com@tcp(erpyjun2.cafe24.com:3306)/day_data")
	if err != nil {
		panic(err.Error()) // Just for example purpose. You should use proper error handling instead of panic
	}
	defer db.Close()

	rows, err := db.Query("select s_code from acc_money group by s_code")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	total := 0
	var s_code string
	var arr_code[2100] string

	for rows.Next() {
		err := rows.Scan(&s_code)
		if err != nil {
			log.Fatal(err)
		}
		arr_code[total] = s_code
		//fmt.Printf("code [%d][%s]\n",total, arr_code[total])
		total++
	}
	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}

	//////////////////////////// select analysis ////////////////////////////
	var totalPrint int
	var s_money float32
	var totalMoney float32
	var s_date string
	for i := 0; i < total; i++ {
		sql := fmt.Sprintf("select s_money, s_code, s_date from acc_money where s_code='%s' and s_date <= '20180102' and s_date >= '20171201' order by s_date desc", arr_code[i])

		rows, err := db.Query(sql)
		if err != nil {
			log.Fatal(err)
		}

		totalMoney = 0
		totalPrint++

		for rows.Next() {
			err := rows.Scan(&s_money, &s_code, &s_date)
			if err != nil {
				log.Fatal(err)
			}
			totalMoney += s_money
			//fmt.Printf("[%d] code [%s][%f][%s]\n",totalPrint ,s_code, s_money, s_date)
		}
		//fmt.Printf("====================================\n")
		fmt.Printf("[%d] %f %s\n",totalPrint, totalMoney, s_code)
		stockMap[s_code] = totalMoney

		err = rows.Err()
		if err != nil {
			log.Fatal(err)
		}
	}

	//////////////////////////// sort & print ////////////////////////////
	i := 0
	p := make(PairList, len(stockMap))
	for k, v := range stockMap {
		p[i] = Pair{k, v}
		i++
	}
	sort.Sort(p)

	fmt.Printf("==========================================\n")
	var sqlQuery string
	var stockName string
	for _, k := range p {
		sqlQuery = fmt.Sprintf("select s_name from stock_main where s_code='%s'", k.Key)
		rows, err := db.Query(sqlQuery)
		if err != nil {
			log.Fatal(err)
		}
		for rows.Next() {
			err := rows.Scan(&stockName)
			if err != nil {
				log.Fatal(err)
			}
		}

		fmt.Printf("[%s][%s] %f\n", k.Key, stockName, k.Value)
	}

	fmt.Println("end")
}

package storage

import (
	"database/sql"
	"fmt"
)

//Rubric entity
type Rubric struct {
	ID         int
	Name, Slug string
}

//Storage main struct
type Storage struct {
	db *sql.DB
}

//NewStorage create storage
func NewStorage(mysqlUser, mysqlPass, mysqlDB string) (*Storage, error) {
	dsn := fmt.Sprintf("%s:%s@unix(/run/mysqld/mysqld.sock)/%s?parseTime=true", mysqlUser, mysqlPass, mysqlDB)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	return &Storage{db: db}, nil
}

//GetEntries return rubrics from db
func (s Storage) GetEntries() ([]Rubric, error) {

	rows, err := s.db.Query("SELECT name,id,cpu AS slug FROM rubrics WHERE counter > 0")

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var rubrics []Rubric

	for rows.Next() {

		var (
			name, slug string
			id         int
		)

		err = rows.Scan(&name, &id, &slug)

		if err != nil {
			return nil, err
		}
		rubrics = append(rubrics, Rubric{ID: id, Name: name, Slug: slug})
	}
	return rubrics, nil
}

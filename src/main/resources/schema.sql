create table if not exists Taco_Order (
    id identity primary key,
    delivery_Name varchar(50) not null,
    delivery_Street varchar(50) not null,
    delivery_City varchar(50) not null,
    delivery_State varchar(2) not null,
    delivery_Zip varchar(10) not null,
    cc_number varchar(16) not null,
    cc_expiration varchar(5) not null,
    cc_cvv varchar(3) not null,
    placed_at timestamp not null
    );

create table if not exists Taco (
    id identity primary key,
    name varchar(50) not null,
    taco_order bigint not null,
    created_at timestamp not null
    );

create table if not exists Ingredient (
    id varchar(4) not null,
    name varchar(25) not null,
    type varchar(10) not null,
    primary key (id)
    );

create table if not exists Ingredient_Ref (
     taco bigint not null,
     ingredient varchar(4) not null
    );

alter table Taco
    add foreign key (taco_order) references Taco_Order(id);

alter table Ingredient_Ref
    add foreign key (taco) references Taco(id);

alter table Ingredient_Ref
    add foreign key (ingredient) references Ingredient(id);
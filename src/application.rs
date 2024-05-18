use actix::prelude::*;
use uuid::Uuid;

use crate::{
    messages::{GetResult, MessagePrepare},
    operations::{Arguments, Operation, Table},
    repository::Repository,
    runtime::Runtime,
};

pub(crate) async fn single_repository_transaction(
    repository: &Addr<Repository>,
    ops: Vec<Operation>,
    runtime: &mut Runtime,
) -> anyhow::Result<Option<Table>> {
    let tid = Uuid::new_v4();
    let args = Arguments {
        timestamp: runtime.now(),
        operations: ops,
    };
    let msg = MessagePrepare::Single(tid.clone(), args);
    // TODO: should `MailboxError` be transformed more explicitly?
    let _commit_vote = repository.send(msg).await?;

    repository.send(GetResult(tid)).await?
}

pub(crate) async fn indep_repository_transaction(
    repositories: Vec<Addr<Repository>>,
    ops: Vec<Vec<Operation>>,
    runtime: &mut Runtime,
) -> anyhow::Result<Vec<Option<Table>>> {
    let tid = Uuid::new_v4();
    let ts = runtime.now();

    let mut votes = vec![];
    for (repository, ops) in repositories.iter().zip(ops) {
        let args = Arguments {
            timestamp: ts,
            operations: ops,
        };
        let msg = MessagePrepare::Indep(tid.clone(), args.clone(), repositories.len());
        // TODO: should `MailboxError` be transformed more explicitly?
        votes.push(repository.send(msg).await??);
    }

    for (repo, vote) in repositories.iter().zip(votes) {
        // TODO: rm .clone()
        let msg = MessagePrepare::IndepParticipants(tid, vote, repositories.clone());
        let _ = repo.send(msg).await?;
    }

    let mut results = vec![];
    for repository in repositories {
        results.push(repository.send(GetResult(tid)).await??);
    }

    // XXX: this should be a flatten of response?
    Ok(results)
}

pub(crate) async fn coord_repository_transaction(
    repositories: Vec<Addr<Repository>>,
    ops: Vec<Vec<Operation>>,
    runtime: &mut Runtime,
) -> anyhow::Result<Vec<Option<Table>>> {
    let tid = Uuid::new_v4();
    let ts = runtime.now();

    let mut votes = vec![];
    for (repository, ops) in repositories.iter().zip(ops) {
        let args = Arguments {
            timestamp: ts,
            operations: ops,
        };
        let msg = MessagePrepare::Coord(tid.clone(), args.clone(), repositories.len());
        // TODO: should `MailboxError` be transformed more explicitly?
        votes.push(repository.send(msg).await??);
    }

    for (repo, vote) in repositories.iter().zip(votes) {
        // TODO: rm .clone()
        let msg = MessagePrepare::CoordParticipants(tid, vote, repositories.clone());
        let _ = repo.send(msg).await?;
    }

    let mut results = vec![];
    for repository in repositories {
        results.push(repository.send(GetResult(tid)).await??);
    }

    // XXX: this should be a flatten of response?
    Ok(results)
}

mod tests {
    use super::*;
    use actix::prelude::*;

    use crate::{
        operations::Operation,
        operations::{Expr, Statement},
        repository::Repository,
        runtime::Runtime,
    };

    async fn create_customer_product_tables(
        runtime: &mut Runtime,
    ) -> (Addr<Repository>, Addr<Repository>) {
        let customer = Repository::new("customer".to_string()).start();
        let operations = vec![
            Operation::Statement(Statement::Create(1, Box::new(Expr::Value((1, 1))))),
            Operation::Statement(Statement::Create(2, Box::new(Expr::Value((2, 2))))),
            Operation::Statement(Statement::Create(3, Box::new(Expr::Value((3, 3))))),
        ];

        let cust = single_repository_transaction(&customer.clone(), operations, runtime).await;
        println!("Adding customer info. Result: {:?}", cust);

        let prod = Repository::new("product".to_string()).start();
        let operations = vec![
            Operation::Statement(Statement::Create(0, Box::new(Expr::Value((4, 4))))),
            Operation::Statement(Statement::Create(1, Box::new(Expr::Value((5, 5))))),
            Operation::Statement(Statement::Create(2, Box::new(Expr::Value((6, 6))))),
        ];
        let resp = single_repository_transaction(&prod.clone(), operations, runtime).await;
        println!("Adding product info. Result: {:?}", resp);

        (customer, prod)
    }

    #[actix_rt::test]
    async fn test_indep_ok() {
        let mut runtime = Runtime::new();
        let (customer, product) = create_customer_product_tables(&mut runtime).await;
        let operations = vec![
            vec![Operation::Expr(Expr::Read(1))],
            vec![Operation::Expr(Expr::Read(1))],
        ];

        let res = indep_repository_transaction(
            vec![customer.clone(), product.clone()],
            operations,
            &mut runtime,
        )
        .await;

        assert!(res.is_ok());
        let _ = res.unwrap().iter().map(|r| assert!(r.is_some()));
        println!("Reading a key that exists on both. Should be ok.");
    }

    #[actix_rt::test]
    async fn test_indep_not_valid_key() {
        let mut runtime = Runtime::new();
        let (customer, product) = create_customer_product_tables(&mut runtime).await;
        let operations = vec![
            vec![Operation::Expr(Expr::Read(1))],
            vec![Operation::Expr(Expr::Read(4))],
        ];

        let res = indep_repository_transaction(
            vec![customer.clone(), product.clone()],
            operations,
            &mut runtime,
        )
        .await;
        assert!(res.is_err());
        println!("Read transaction failed because of a primary key violation. Should be err.");
    }

    #[actix_rt::test]
    async fn test_indep_update_failed_because_of_primary_key_violation() {
        let mut runtime = Runtime::new();
        let (customer, product) = create_customer_product_tables(&mut runtime).await;
        let operations = vec![Operation::Expr(Expr::Read(1))];

        let cust_before = single_repository_transaction(&customer, operations, &mut runtime).await;

        let operations = vec![
            vec![Operation::Statement(Statement::Update(
                1,
                Box::new(Expr::Value((1000, 1000))),
            ))],
            vec![Operation::Expr(Expr::Read(4))],
        ];

        let res = indep_repository_transaction(
            vec![customer.clone(), product.clone()],
            operations,
            &mut runtime,
        )
        .await;
        assert!(res.is_err());

        let operations = vec![Operation::Expr(Expr::Read(1))];

        let cust = single_repository_transaction(&customer, operations, &mut runtime).await;

        assert!(cust.is_ok());
        assert_eq!(cust_before.unwrap(), cust.unwrap());
        println!("An `abort` transaction has not effect on a repo");
    }

    #[actix_rt::test]
    async fn test_coord_ok() {
        let mut runtime = Runtime::new();
        let (customer, product) = create_customer_product_tables(&mut runtime).await;

        let operations = vec![
            vec![Operation::Statement(Statement::Update(
                1,
                Box::new(Expr::Value((10, 10))),
            ))],
            vec![Operation::Statement(Statement::Update(
                1,
                Box::new(Expr::Value((40, 40))),
            ))],
        ];

        let res = coord_repository_transaction(
            vec![customer.clone(), product.clone()],
            operations,
            &mut runtime,
        )
        .await;
        assert!(res.is_ok());

        let operations = vec![Operation::Expr(Expr::Read(1))];
        let cust = single_repository_transaction(&customer, operations, &mut runtime).await;

        let operations = vec![Operation::Expr(Expr::Read(1))];
        let prod = single_repository_transaction(&product, operations, &mut runtime).await;

        assert!(cust.is_ok());
        assert!(prod.is_ok());
        assert_eq!(cust.unwrap(), Some((10, 10)));
        assert_eq!(prod.unwrap(), Some((40, 40)));
        println!("Coordinated update succeeded.");
    }

    #[actix_rt::test]
    async fn test_coord_should_fail() {
        let mut runtime = Runtime::new();
        let (customer, product) = create_customer_product_tables(&mut runtime).await;

        let operations = vec![
            vec![Operation::Statement(Statement::Update(
                1,
                Box::new(Expr::Value((10, 10))),
            ))],
            vec![
                Operation::Statement(Statement::Update(1, Box::new(Expr::Value((40, 40))))),
                // Should fail.
                Operation::Expr(Expr::Read(5)),
            ],
        ];

        let res = coord_repository_transaction(
            vec![customer.clone(), product.clone()],
            operations,
            &mut runtime,
        )
        .await;
        assert!(res.is_err());

        let operations = vec![Operation::Expr(Expr::Read(1))];
        let cust = single_repository_transaction(&customer, operations, &mut runtime).await;

        let operations = vec![Operation::Expr(Expr::Read(1))];
        let prod = single_repository_transaction(&product, operations, &mut runtime).await;

        assert!(cust.is_ok());
        assert!(prod.is_ok());
        assert_eq!(cust.unwrap(), Some((1, 1)));
        assert_eq!(prod.unwrap(), Some((5, 5)));
        println!("Coordinated fail to update due to primary key violation.");
    }
}

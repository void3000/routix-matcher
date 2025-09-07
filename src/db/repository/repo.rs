pub trait Repository {
    type Pool;

    fn new(pool: Self::Pool) -> Self;
}

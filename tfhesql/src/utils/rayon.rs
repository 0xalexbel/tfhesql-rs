#[inline]
pub fn rayon_join5<A, B, C, D, E, RA, RB, RC, RD, RE>(
    oper_a: A,
    oper_b: B,
    oper_c: C,
    oper_d: D,
    oper_e: E,
) -> (RA, RB, RC, RD, RE)
where
    A: FnOnce() -> RA + Send,
    B: FnOnce() -> RB + Send,
    C: FnOnce() -> RC + Send,
    D: FnOnce() -> RD + Send,
    E: FnOnce() -> RE + Send,
    RA: Send,
    RB: Send,
    RC: Send,
    RD: Send,
    RE: Send,
{
    let res = rayon::join(
        || rayon::join(oper_a, oper_b),
        || rayon_join3(oper_c, oper_d, oper_e),
    );
    (res.0 .0, res.0 .1, res.1 .0, res.1 .1, res.1 .2)
}

#[inline]
pub fn rayon_join4<A, B, C, D, RA, RB, RC, RD>(
    oper_a: A,
    oper_b: B,
    oper_c: C,
    oper_d: D,
) -> (RA, RB, RC, RD)
where
    A: FnOnce() -> RA + Send,
    B: FnOnce() -> RB + Send,
    C: FnOnce() -> RC + Send,
    D: FnOnce() -> RD + Send,
    RA: Send,
    RB: Send,
    RC: Send,
    RD: Send,
{
    let res = rayon::join(
        || rayon::join(oper_a, oper_b),
        || rayon::join(oper_c, oper_d),
    );
    (res.0 .0, res.0 .1, res.1 .0, res.1 .1)
}

#[inline]
pub fn rayon_join3<A, B, C, RA, RB, RC>(oper_a: A, oper_b: B, oper_c: C) -> (RA, RB, RC)
where
    A: FnOnce() -> RA + Send,
    B: FnOnce() -> RB + Send,
    C: FnOnce() -> RC + Send,
    RA: Send,
    RB: Send,
    RC: Send,
{
    let res = rayon::join(|| rayon::join(oper_a, oper_b), oper_c);
    (res.0 .0, res.0 .1, res.1)
}

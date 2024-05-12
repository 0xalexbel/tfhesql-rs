use crate::bitops::*;
use crate::types::*;
use crate::default_into::DefaultInto;

////////////////////////////////////////////////////////////////////////////////
// OptimizedBool
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Copy)]
pub struct OptimizedBool<B> {
    value: Option<B>,
    is_true: bool,
    is_false: bool,
}

impl<T> UIntType for OptimizedBool<T>
where
    T: BooleanType,
{
    #[inline(always)]
    fn get_zero() -> Self {
        OptimizedBool::<T>::get_false()
    }
    #[inline(always)]
    fn get_max() -> Self {
        OptimizedBool::<T>::get_true()
    }
}

impl<T> DebugToString for OptimizedBool<T>
where
    T: DebugToString,
{
    fn debug_to_string(&self) -> String {
        if self.is_true {
            true.debug_to_string()
        } else if self.is_false {
            false.debug_to_string()
        } else {
            self.debug_to_string()
        }
    }
}

impl<T> DefaultInto<OptimizedBool<T>> for OptimizedBool<T>
where
    T: BooleanType,
{
    fn default_into() -> OptimizedBool<T> {
        OptimizedBool {
            value: None,
            is_true: false,
            is_false: true,
        }
    }
}

impl<T> BooleanType for OptimizedBool<T>
where
    T: BooleanType,
{
    fn get_true() -> Self {
        OptimizedBool::<T> {
            value: None,
            is_false: false,
            is_true: true,
        }
    }

    fn get_false() -> Self {
        OptimizedBool::<T> {
            value: None,
            is_false: true,
            is_true: false,
        }
    }
}

#[cfg(test)]
impl<B> OptimizedBool<B> {
    #[inline(always)]
    pub fn is_unknown(&self) -> bool {
        !self.is_false && !self.is_true
    }
    #[inline(always)]
    pub fn is_true(&self) -> bool {
        self.is_true
    }
    #[inline(always)]
    pub fn is_false(&self) -> bool {
        self.is_false
    }
}

impl<T> RefBitAnd for OptimizedBool<T>
where
    T: RefBitAnd<Output = T> + Clone,
{
    type Output = Self;

    #[inline(always)]
    fn ref_bitand(&self, rhs: OptimizedBool<T>) -> Self::Output {
        if self.is_false {
            self.clone()
        } else if rhs.is_false || self.is_true {
            rhs.clone()
        } else if rhs.is_true {
            self.clone()
        } else {
            OptimizedBool {
                value: Some(
                    self.value
                        .as_ref()
                        .unwrap()
                        .refref_bitand(rhs.value.as_ref().unwrap()),
                ),
                is_true: false,
                is_false: false,
            }
        }
    }

    #[inline(always)]
    fn refref_bitand(&self, rhs: &OptimizedBool<T>) -> Self::Output {
        if self.is_false {
            self.clone()
        } else if rhs.is_false || self.is_true {
            rhs.clone()
        } else if rhs.is_true {
            self.clone()
        } else {
            OptimizedBool {
                value: Some(
                    self.value
                        .as_ref()
                        .unwrap()
                        .refref_bitand(rhs.value.as_ref().unwrap()),
                ),
                is_true: false,
                is_false: false,
            }
        }
    }
}

impl<T> RefBitAnd<bool> for OptimizedBool<T>
where
    T: RefBitAnd<Output = T> + Clone + BooleanType,
{
    type Output = Self;

    #[inline(always)]
    fn ref_bitand(&self, rhs: bool) -> Self::Output {
        if !rhs {
            OptimizedBool {
                value: None,
                is_true: false,
                is_false: true,
            }
        } else {
            self.clone()
        }
    }

    #[inline(always)]
    fn refref_bitand(&self, rhs: &bool) -> Self::Output {
        if !*rhs {
            OptimizedBool {
                value: None,
                is_true: false,
                is_false: true,
            }
        } else {
            self.clone()
        }
    }
}

impl<T> RefBitOr for OptimizedBool<T>
where
    T: RefBitOr<Output = T> + Clone,
{
    type Output = Self;

    #[inline(always)]
    fn ref_bitor(&self, rhs: OptimizedBool<T>) -> Self::Output {
        if self.is_true {
            self.clone()
        } else if rhs.is_true || self.is_false {
            rhs.clone()
        } else if rhs.is_false {
            self.clone()
        } else {
            OptimizedBool {
                value: Some(
                    self.value
                        .as_ref()
                        .unwrap()
                        .refref_bitor(rhs.value.as_ref().unwrap()),
                ),
                is_true: false,
                is_false: false,
            }
        }
    }

    #[inline(always)]
    fn refref_bitor(&self, rhs: &OptimizedBool<T>) -> Self::Output {
        if self.is_true {
            self.clone()
        } else if rhs.is_true || self.is_false {
            rhs.clone()
        } else if rhs.is_false {
            self.clone()
        } else {
            OptimizedBool {
                value: Some(
                    self.value
                        .as_ref()
                        .unwrap()
                        .refref_bitor(rhs.value.as_ref().unwrap()),
                ),
                is_true: false,
                is_false: false,
            }
        }
    }
}

impl<T> RefBitOr<bool> for OptimizedBool<T>
where
    T: RefBitOr<Output = T> + Clone + BooleanType,
{
    type Output = Self;

    #[inline(always)]
    fn ref_bitor(&self, rhs: bool) -> Self::Output {
        if rhs {
            OptimizedBool {
                value: None,
                is_true: true,
                is_false: false,
            }
        } else {
            self.clone()
        }
    }

    #[inline(always)]
    fn refref_bitor(&self, rhs: &bool) -> Self::Output {
        if *rhs {
            OptimizedBool {
                value: None,
                is_true: true,
                is_false: false,
            }
        } else {
            self.clone()
        }
    }
}

impl<T> RefNot for OptimizedBool<T>
where
    T: RefNot + Clone,
{
    fn ref_not(&self) -> Self {
        if self.is_false || self.is_true {
            let mut a = self.clone();
            a.is_true = self.is_false;
            a.is_false = self.is_true;
            a
        } else {
            OptimizedBool {
                value: Some(self.value.as_ref().unwrap().ref_not()),
                is_true: false,
                is_false: false,
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// Mask<OptimizedBool<T>>
////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
use crate::uint::mask::Mask;

#[cfg(test)]
impl<B> Mask<OptimizedBool<B>> {
    fn is_none(&self) -> bool {
        for i in 0..self.mask.len() {
            if self.mask[i].is_true() || self.mask[i].is_unknown() {
                return false;
            }
        }
        true
    }
    fn is_all(&self) -> bool {
        for i in 0..self.mask.len() {
            if self.mask[i].is_false() || self.mask[i].is_unknown() {
                return false;
            }
        }
        true
    }
}

////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod test {
    use tfhe::FheBool;
    use super::*;

    #[test]
    fn test() {
        let mask1 = Mask::<OptimizedBool<FheBool>>::none(4);
        let mask2 = Mask::<OptimizedBool<FheBool>>::none(4);
        let a = mask1.refref_bitand(&mask2);
        assert!(a.is_none());

        let mask1 = Mask::<OptimizedBool<FheBool>>::all(4);
        let mask2 = Mask::<OptimizedBool<FheBool>>::all(4);
        let a = mask1.refref_bitand(&mask2);
        assert!(a.is_all());
    }
}

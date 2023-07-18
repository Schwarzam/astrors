use std::ffi::CString;
use std::os::raw::c_char;
use std::any::type_name;
use std::mem;

extern {
    fn fits_hcompress(a: *mut i32, ny: i32, nx: i32, scale: i32, output: *mut c_char, nbytes: *mut i64, status: *mut i32) -> i32;

    fn fits_hdecompress(input: *mut c_char, smooth: i32, a: *mut i32, ny: *mut i32, nx: *mut i32, 
                     scale: *mut i32, status: *mut i32) -> i32;

    fn testando(a: *mut i32, ny: i32, nx: i32, scale: i32, output: *mut c_char, nbytes: *mut i64, status: *mut i32); 
}

fn print_type_of<T>(_: &T) {
    println!("{}", std::any::type_name::<T>())
}
fn main() {
    let mut a = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 2, 3, 41, 2, 1, 3, 4, 1, 2, 3, 4, 1, 2, 3];

    println!("a = {:?}", a.len());
    let _tamanho = a.len() as i32;

    // unsafe {
    //     testando(a.as_mut_ptr(), tamanho);
    // }
    println!("a = {:?}", a);
   
    let mut nbytes : i64 = 24 * 4;
    let mut status : i32 = 0;
    // let mut output : char = String::new();
   
    const BUFFER_SIZE: usize = 100;
    let mut output_buffer: Vec<u8> = vec![0; BUFFER_SIZE];
    let output_ptr = output_buffer.as_mut_ptr() as *mut c_char;
    std::mem::forget(output_buffer); // Prevent buffer from being deallocated
    

    let mut scale : i32 = 10;
    unsafe {
        fits_hcompress(a.as_mut_ptr().cast(), 24, 1, scale, output_ptr, &mut nbytes, &mut status);
    }
    
    println!("nbytes = {:?}", nbytes);
    println!("a = {:?}", a);
     
    let output_string = unsafe { CString::from_raw(output_ptr) };
    // let output_string = output_string.to_str().expect("Invalid UTF-8");
    print_type_of(&output_string);
    println!("raw_ptr = {:?}", output_string);
    for byte in output_string.as_bytes() {
        println!("Byte: {}", byte);
    }

    //I need a empty 24x1 array 
    let mut b = [0; 24];
    let mut output_buffer2: Vec<u8> = vec![0; BUFFER_SIZE];
    output_buffer2[0] = 221;
    output_buffer2[1] = 153;
    let output_ptr2 = output_buffer2.as_mut_ptr() as *mut c_char;


    std::mem::forget(output_buffer2); // Prevent buffer from being deallocated

    unsafe {
        fits_hdecompress(output_ptr2, 0, b.as_mut_ptr().cast(), &mut 24, &mut 1, &mut scale, &mut status);
    }
    
    println!("b = {:?}", b);
}

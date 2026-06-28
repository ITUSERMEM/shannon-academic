use shannon_agent_core::metrics;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

#[test]
fn test_concurrent_metrics_initialization() {
    // Test that multiple threads can safely initialize metrics
    let init_count = Arc::new(AtomicUsize::new(0));
    let error_count = Arc::new(AtomicUsize::new(0));

    let mut handles = vec![];

    for i in 0..10 {
        let init_count = init_count.clone();
        let error_count = error_count.clone();

        let handle = thread::spawn(move || {
            // Each thread tries to initialize metrics
            match metrics::init_metrics() {
                Ok(_) => {
                    init_count.fetch_add(1, Ordering::SeqCst);
                    println!("Thread {} initialized metrics (idempotent OK)", i);
                }
                Err(e) => {
                    // No error is expected in idempotent init
                    error_count.fetch_add(1, Ordering::SeqCst);
                    eprintln!("Thread {} unexpected init error: {}", i, e);
                }
            }

            // Try to use metrics after initialization
            let timer = metrics::TaskTimer::new("test");
            thread::sleep(Duration::from_millis(10));
            timer.complete("success", Some(100));
        });

        handles.push(handle);
    }

    // Wait for all threads
    for handle in handles {
        handle.join().unwrap();
    }

    // All threads should see Ok(()) due to idempotent initialization
    assert_eq!(
        init_count.load(Ordering::SeqCst),
        10,
        "All threads should observe successful initialization"
    );
    assert_eq!(
        error_count.load(Ordering::SeqCst),
        0,
        "No errors should occur during initialization"
    );

    // Verify metrics are accessible
    let metrics_output = metrics::get_metrics();
    assert!(
        !metrics_output.is_empty(),
        "Metrics should be available after initialization"
    );
}

#[test]
fn test_metrics_usage_without_initialization() {
    // Test that metrics can be safely accessed even if not initialized
    // This simulates a scenario where metrics initialization fails

    // Try to use a timer without initialization
    let timer = metrics::TaskTimer::new("test_mode");
    timer.complete("success", Some(42));

    // This should not panic, just silently skip recording
    let metrics_output = metrics::get_metrics();
    // Output might be empty or contain some metrics depending on test order
    println!("Metrics output length: {}", metrics_output.len());
}

#[test]
fn test_oncelock_double_initialization() {
    // Test that OnceLock properly prevents double initialization
    use std::sync::OnceLock;

    static TEST_METRIC: OnceLock<String> = OnceLock::new();

    // First initialization should succeed
    let result1 = TEST_METRIC.set("first".to_string());
    assert!(result1.is_ok(), "First set should succeed");

    // Second initialization should fail
    let result2 = TEST_METRIC.set("second".to_string());
    assert!(result2.is_err(), "Second set should fail");

    // Value should be the first one
    assert_eq!(TEST_METRIC.get().unwrap(), "first");
}

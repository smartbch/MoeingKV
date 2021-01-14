#pragma once
#include <atomic>
#include <memory>

// It is a container for one pointer. One writer can change the object this pointer points to, and replace
// this pointer with another pointer. Many readers can read the object this pointer points to. Once a 
// pointer is added into this container, its ownership is also transferred.
// One writer and many readers can work concurrently. And you must ensure there is only one writer.
template<typename T>
class ptr_for_rent {
	static constexpr uint64_t TO_DEL_THIS() {
		return (uint64_t(1)<<63); // The highest bit indicates "deleting it sometime later".
	}
	// To delete a useless pointer properly, we need to track its usage in 'status'.
	struct releasable {
		std::atomic_ullong status;
		T* ptr;
		releasable(T* p): status(0), ptr(p) {}
		~releasable() {
			delete ptr;
		}
		// Request to delete 'ptr'. Returns true if it can be deleted now.
		bool request_to_release() {
			auto old_value = std::atomic_fetch_or(&status, TO_DEL_THIS());
			return old_value == 0; //true if no others are renting it
		}
		// Increase the reference counter.
		void begin_renting() {
			std::atomic_fetch_add(&status, uint64_t(1));
		}
		// Decrease the reference counter and return true if 'ptr' can be deleted now.
		bool end_renting_and_check_release_request() {
			auto old_value = std::atomic_fetch_sub(&status, uint64_t(1));
			return old_value == (TO_DEL_THIS() + 1); //true if I am the last one renting it
		}
	};

	std::atomic<releasable*> data;
	// Try to delete 'ptr'. If it cannot be deleted now, mark it as "to-be-deleted" and it will be
	// deleted sometime later.
	void try_delete(releasable* ptr) {
		bool ok = ptr->request_to_release();
		if(ok) delete ptr;
	}
public:
	ptr_for_rent(): data(nullptr) {}
	~ptr_for_rent() {
		try_delete(data.load());
	}
	bool is_empty() {
		return data.load() == nullptr;
	}
	// Replace the old pointer with 'new_ptr' and then delete the old pointer.
	void replace(T* new_ptr) {
		auto obj = new releasable(new_ptr);
		releasable* ptr = data.exchange(obj);
		try_delete(ptr);
	}
	// Rent the pointer for read and write.
	template<typename F>
	void rent(F f) {
		auto ptr = data.load();
		ptr->begin_renting();
		f(ptr->ptr);
		bool need_to_release = ptr->end_renting_and_check_release_request();
		if(need_to_release) delete ptr;
	}
	// Rent the pointer for read.
	template<typename F>
	void rent_const(F f) {
		auto ptr = data.load();
		ptr->begin_renting();
		f(reinterpret_cast<const T*>(ptr->ptr));
		bool need_to_release = ptr->end_renting_and_check_release_request();
		if(need_to_release) delete ptr;
	}
};


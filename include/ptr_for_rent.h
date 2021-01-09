#pragma once
#include <atomic>
#include <memory>

template<typename T>
class ptr_for_rent {
	static constexpr uint64_t TO_DEL_THIS() {
		return (uint64_t(1)<<63);
	}
	struct releasable {
		std::atomic_ullong status;
		T* ptr;
		releasable(T* p): status(0), ptr(p) {}
		~releasable() {
			delete ptr;
		}
		bool request_to_release() {
			auto old_value = std::atomic_fetch_or(&status, TO_DEL_THIS());
			return old_value == 0; //true if no others are renting it
		}
		void begin_renting() {
			std::atomic_fetch_add(&status, uint64_t(1));
		}
		bool end_renting_and_check_release_request() {
			auto old_value = std::atomic_fetch_sub(&status, uint64_t(1));
			return old_value == (TO_DEL_THIS() + 1); //true if I am the last one renting it
		}
	};

	std::atomic<releasable*> data;
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
	void replace(T* new_ptr) {
		auto obj = new releasable(new_ptr);
		releasable* ptr = data.exchange(obj);
		try_delete(ptr);
	}
	template<typename F>
	void rent(F f) {
		auto ptr = data.load();
		ptr->begin_renting();
		f(ptr->ptr);
		bool need_to_release = ptr->end_renting_and_check_release_request();
		if(need_to_release) delete ptr;
	}
	template<typename F>
	void rent_const(F f) {
		auto ptr = data.load();
		ptr->begin_renting();
		f(reinterpret_cast<const T*>(ptr->ptr));
		bool need_to_release = ptr->end_renting_and_check_release_request();
		if(need_to_release) delete ptr;
	}
};

